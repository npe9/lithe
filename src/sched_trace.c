/* Copyright (c) 2026 The Regents of the University of California
 * Lithe scheduling decision + latency tracing implementation.
 *
 * Design:
 *  - Default OFF (LITHE_SCHED_TRACE unset/0): one cached int load per emit site.
 *  - Per-vcore lock-free ring (single producer per vcore = that vcore's hart).
 *  - Dump on atexit, SIGUSR2, or lithe_sched_trace_dump().
 *  - No pthread sync, no LD_PRELOAD; plain C11 atomics only.
 */

#include "sched_trace.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <parlib/vcore.h>

#ifndef LITHE_SCHED_TRACE_MAX_VCORES
#define LITHE_SCHED_TRACE_MAX_VCORES MAX_VCORES
#endif

#ifndef LITHE_SCHED_TRACE_RING_DEFAULT
#define LITHE_SCHED_TRACE_RING_DEFAULT 4096u
#endif

/* enabled: -1 uninit, 0 off, 1 on */
static int st_enabled = -1;
static int st_inited;
static unsigned int st_ring_cap; /* power of 2 */
static unsigned int st_ring_mask;
static unsigned int st_n_vcores;

static lithe_sched_trace_rec_t *st_rings[LITHE_SCHED_TRACE_MAX_VCORES];
static uint64_t st_write_seq[LITHE_SCHED_TRACE_MAX_VCORES];
static uint64_t st_dropped[LITHE_SCHED_TRACE_MAX_VCORES];
static uint64_t st_last_ns[LITHE_SCHED_TRACE_MAX_VCORES];

static lithe_sched_trace_counters_t st_ctrs;
static char st_path[PATH_MAX];
static struct sigaction st_old_sigusr2;
static int st_sigusr2_installed;

static uint64_t st_now_ns(void)
{
	struct timespec ts;
#if defined(CLOCK_MONOTONIC_RAW)
	if (clock_gettime(CLOCK_MONOTONIC_RAW, &ts) == 0)
		return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
#endif
	clock_gettime(CLOCK_MONOTONIC, &ts);
	return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static unsigned int st_next_pow2(unsigned int v)
{
	unsigned int p = 1;
	if (v < 64)
		return 64;
	while (p < v && p < (1u << 20))
		p <<= 1;
	return p;
}

static void st_resolve_path(void)
{
	const char *e = getenv("LITHE_SCHED_TRACE_PATH");
	if (e && *e) {
		snprintf(st_path, sizeof(st_path), "%s", e);
		return;
	}
	snprintf(st_path, sizeof(st_path), "lithe_sched_trace.%d.bin", (int)getpid());
}

static void st_on_sigusr2(int sig)
{
	(void)sig;
	(void)lithe_sched_trace_dump();
}

static void st_atexit_dump(void)
{
	if (st_enabled == 1)
		(void)lithe_sched_trace_dump();
}

static void st_init_once(void)
{
	unsigned int i, cap;
	const char *e;

	if (st_inited)
		return;

	st_resolve_path();
	cap = LITHE_SCHED_TRACE_RING_DEFAULT;
	e = getenv("LITHE_SCHED_TRACE_RING");
	if (e && *e) {
		char *end = NULL;
		unsigned long v = strtoul(e, &end, 10);
		if (end != e && v >= 64 && v <= (1ul << 20))
			cap = (unsigned int)v;
	}
	st_ring_cap = st_next_pow2(cap);
	st_ring_mask = st_ring_cap - 1;

	st_n_vcores = (unsigned int)max_vcores();
	if (st_n_vcores == 0 || st_n_vcores > LITHE_SCHED_TRACE_MAX_VCORES)
		st_n_vcores = LITHE_SCHED_TRACE_MAX_VCORES;

	for (i = 0; i < st_n_vcores; i++) {
		st_rings[i] = calloc(st_ring_cap, sizeof(lithe_sched_trace_rec_t));
		if (!st_rings[i]) {
			fprintf(stderr, "[LITHE_SCHED_TRACE] calloc failed for vcore %u "
			        "(cap=%u); tracing disabled\n", i, st_ring_cap);
			st_enabled = 0;
			st_inited = 1;
			return;
		}
		st_write_seq[i] = 0;
		st_dropped[i] = 0;
		st_last_ns[i] = 0;
	}

	memset(&st_ctrs, 0, sizeof(st_ctrs));

	if (!st_sigusr2_installed) {
		struct sigaction sa;
		memset(&sa, 0, sizeof(sa));
		sa.sa_handler = st_on_sigusr2;
		sigemptyset(&sa.sa_mask);
		sa.sa_flags = SA_RESTART;
		if (sigaction(SIGUSR2, &sa, &st_old_sigusr2) == 0)
			st_sigusr2_installed = 1;
	}
	atexit(st_atexit_dump);

	st_inited = 1;
	fprintf(stderr,
	        "[LITHE_SCHED_TRACE] enabled ring_cap=%u n_vcores=%u path=%s "
	        "(dump on atexit/SIGUSR2/lithe_sched_trace_dump)\n",
	        st_ring_cap, st_n_vcores, st_path);
}

int lithe_sched_trace_enabled(void)
{
	if (st_enabled < 0) {
		const char *e = getenv("LITHE_SCHED_TRACE");
		st_enabled = (e && *e && atoi(e)) ? 1 : 0;
		if (st_enabled)
			st_init_once();
	}
	return st_enabled == 1;
}

const char *lithe_sched_trace_ev_name(uint8_t ev)
{
	switch (ev) {
	case LITHE_ST_HART_ENTER: return "hart_enter";
	case LITHE_ST_DEQUEUE_LOCAL: return "dequeue_local";
	case LITHE_ST_STEAL_ATTEMPT: return "steal_attempt";
	case LITHE_ST_STEAL_SUCCESS: return "steal_success";
	case LITHE_ST_STEAL_FAIL: return "steal_fail";
	case LITHE_ST_HART_GRANT: return "hart_grant";
	case LITHE_ST_HART_RETURN: return "hart_return";
	case LITHE_ST_CTX_YIELD: return "context_yield";
	case LITHE_ST_CTX_BLOCK: return "context_block";
	case LITHE_ST_CTX_UNBLOCK: return "context_unblock";
	case LITHE_ST_IDLE_SPIN: return "idle_spin";
	case LITHE_ST_IDLE_YIELD: return "idle_yield";
	case LITHE_ST_REACTOR_BLOCK: return "reactor_block";
	case LITHE_ST_PROGRESS_WAKE: return "progress_wake";
	case LITHE_ST_CHILD_ENTER: return "child_enter";
	case LITHE_ST_CHILD_EXIT: return "child_exit";
	case LITHE_ST_WARM_CLAIM: return "warm_claim";
	case LITHE_ST_WARM_PARK: return "warm_park";
	case LITHE_ST_DUMP_MARKER: return "dump_marker";
	default: return "unknown";
	}
}

void lithe_sched_trace_emit(uint8_t ev, void *sched, uint32_t ctx_id,
                            uint8_t decision, int32_t aux)
{
	int vc;
	uint64_t now, seq;
	uint32_t lat;
	lithe_sched_trace_rec_t *slot;

	if (!lithe_sched_trace_enabled())
		return;
	if (!st_inited || st_enabled != 1)
		return;

	vc = vcore_id();
	if (vc < 0 || (unsigned)vc >= st_n_vcores)
		vc = 0;
	if (!st_rings[vc])
		return;

	now = st_now_ns();
	lat = 0;
	if (st_last_ns[vc]) {
		uint64_t d = now - st_last_ns[vc];
		lat = (d > UINT32_MAX) ? UINT32_MAX : (uint32_t)d;
	}
	st_last_ns[vc] = now;

	seq = st_write_seq[vc]++;
	/* Overwrite oldest when full: still advance seq; drop counter for analyzer. */
	if (seq >= st_ring_cap)
		st_dropped[vc]++;

	slot = &st_rings[vc][seq & st_ring_mask];
	slot->ns = now;
	slot->latency_ns = lat;
	slot->vcore_id = (uint16_t)vc;
	slot->ev = ev;
	slot->decision = decision;
	slot->sched_id = (uint32_t)(uintptr_t)sched;
	slot->ctx_id = ctx_id;
	slot->aux = aux;

	__atomic_fetch_add(&st_ctrs.events_emitted, 1, __ATOMIC_RELAXED);
	if (decision & LITHE_ST_DEC_STEAL_LOCAL_NONEMPTY)
		__atomic_fetch_add(&st_ctrs.steal_local_nonempty, 1, __ATOMIC_RELAXED);
	if (decision & LITHE_ST_DEC_IDLE_WITH_RUNNABLE)
		__atomic_fetch_add(&st_ctrs.idle_with_runnable, 1, __ATOMIC_RELAXED);
	if (decision & LITHE_ST_DEC_GRANT_EMPTY_CHILD)
		__atomic_fetch_add(&st_ctrs.grant_empty_child, 1, __ATOMIC_RELAXED);
	if (decision & LITHE_ST_DEC_GRANT_PARENT_RUNNABLE)
		__atomic_fetch_add(&st_ctrs.grant_parent_runnable, 1, __ATOMIC_RELAXED);
	if (decision & LITHE_ST_DEC_YIELD_WITH_PEERS)
		__atomic_fetch_add(&st_ctrs.yield_with_peers, 1, __ATOMIC_RELAXED);
	if (decision & LITHE_ST_DEC_SELF_RESCHED_HOT)
		__atomic_fetch_add(&st_ctrs.self_resched_hot, 1, __ATOMIC_RELAXED);

	switch (ev) {
	case LITHE_ST_HART_ENTER:
		__atomic_fetch_add(&st_ctrs.hart_enter, 1, __ATOMIC_RELAXED);
		break;
	case LITHE_ST_IDLE_YIELD:
		__atomic_fetch_add(&st_ctrs.idle_yield, 1, __ATOMIC_RELAXED);
		break;
	case LITHE_ST_STEAL_ATTEMPT:
		__atomic_fetch_add(&st_ctrs.steal_attempt, 1, __ATOMIC_RELAXED);
		break;
	case LITHE_ST_STEAL_SUCCESS:
		__atomic_fetch_add(&st_ctrs.steal_success, 1, __ATOMIC_RELAXED);
		break;
	default:
		break;
	}
}

void lithe_sched_trace_get_counters(lithe_sched_trace_counters_t *out)
{
	if (!out)
		return;
	*out = st_ctrs;
	out->events_dropped = 0;
	if (st_inited) {
		unsigned int i;
		for (i = 0; i < st_n_vcores; i++)
			out->events_dropped += st_dropped[i];
	}
}

int lithe_sched_trace_dump(void)
{
	int fd;
	unsigned int i;
	lithe_sched_trace_file_hdr_t hdr;
	lithe_sched_trace_counters_t ctrs;
	ssize_t n;

	if (st_enabled != 1 || !st_inited)
		return -1;

	st_resolve_path();
	fd = open(st_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
	if (fd < 0) {
		fprintf(stderr, "[LITHE_SCHED_TRACE] open(%s) failed: %s\n",
		        st_path, strerror(errno));
		return -1;
	}

	memset(&hdr, 0, sizeof(hdr));
	hdr.magic = LITHE_SCHED_TRACE_MAGIC;
	hdr.version = 1;
	hdr.n_vcores = st_n_vcores;
	hdr.ring_cap = st_ring_cap;
	hdr.rec_size = (uint32_t)sizeof(lithe_sched_trace_rec_t);
	hdr.pid = (uint64_t)getpid();

	n = write(fd, &hdr, sizeof(hdr));
	if (n != (ssize_t)sizeof(hdr))
		goto fail;

	for (i = 0; i < st_n_vcores; i++) {
		lithe_sched_trace_vc_hdr_t vh;
		uint64_t seq = st_write_seq[i];
		uint32_t nrec;
		uint64_t start;

		memset(&vh, 0, sizeof(vh));
		vh.vcore_id = i;
		vh.write_seq = seq;
		vh.dropped = st_dropped[i];
		if (seq == 0) {
			nrec = 0;
			start = 0;
		} else if (seq <= st_ring_cap) {
			nrec = (uint32_t)seq;
			start = 0;
		} else {
			nrec = st_ring_cap;
			start = seq - st_ring_cap;
		}
		vh.n_records = nrec;

		n = write(fd, &vh, sizeof(vh));
		if (n != (ssize_t)sizeof(vh))
			goto fail;

		if (nrec && st_rings[i]) {
			uint32_t k;
			for (k = 0; k < nrec; k++) {
				uint64_t idx = (start + k) & st_ring_mask;
				n = write(fd, &st_rings[i][idx],
				          sizeof(lithe_sched_trace_rec_t));
				if (n != (ssize_t)sizeof(lithe_sched_trace_rec_t))
					goto fail;
			}
		}
	}

	lithe_sched_trace_get_counters(&ctrs);
	n = write(fd, &ctrs, sizeof(ctrs));
	if (n != (ssize_t)sizeof(ctrs))
		goto fail;

	close(fd);
	fprintf(stderr,
	        "[LITHE_SCHED_TRACE] dumped %s events=%llu dropped=%llu "
	        "idle_with_runnable=%llu steal_local_nonempty=%llu "
	        "grant_empty_child=%llu yield_with_peers=%llu\n",
	        st_path,
	        (unsigned long long)ctrs.events_emitted,
	        (unsigned long long)ctrs.events_dropped,
	        (unsigned long long)ctrs.idle_with_runnable,
	        (unsigned long long)ctrs.steal_local_nonempty,
	        (unsigned long long)ctrs.grant_empty_child,
	        (unsigned long long)ctrs.yield_with_peers);
	return 0;

fail:
	fprintf(stderr, "[LITHE_SCHED_TRACE] write failed: %s\n", strerror(errno));
	close(fd);
	return -1;
}
