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
#include <parlib/reactor.h>
#include "lithe.h"
#include "context.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <parlib/vcore.h>
#include <parlib/tool.h>

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
		/* "%p" in the path expands to the PID so every process of a job
		 * writes its own file (the driver concatenates the protobufs). */
		const char *pp = strstr(e, "%p");
		if (pp)
			snprintf(st_path, sizeof(st_path), "%.*s%d%s", (int)(pp - e), e,
			         (int)getpid(), pp + 2);
		else
			snprintf(st_path, sizeof(st_path), "%s", e);
		return;
	}
	snprintf(st_path, sizeof(st_path), "lithe_sched_trace.%d.bin", (int)getpid());
}

/* SIGUSR2 lands on whatever thread the kernel picks, usually a vcore thread
 * parked in its small bootstrap stack: dumping there (snprintf + protobuf
 * encode) overflowed it and segfaulted (StarPU+OpenMP hang, 2026-10-19). The
 * handler only pokes a pipe; a dumper pthread with a normal stack does the work. */
static int st_dump_pipe[2] = { -1, -1 };
static void *st_dumper(void *x)
{
	(void)x;
	char b;
	for (;;) {
		ssize_t n = read(st_dump_pipe[0], &b, 1);
		if (n == 1)
			(void)lithe_sched_trace_dump();
		else if (n < 0 && errno != EINTR)
			return NULL;
	}
}
static void st_start_dumper(void)
{
	pthread_t t;
	pthread_attr_t a;
	if (pipe2(st_dump_pipe, O_CLOEXEC) != 0)
		return;
	pthread_attr_init(&a);
	pthread_attr_setdetachstate(&a, PTHREAD_CREATE_DETACHED);
	if (pthread_create(&t, &a, st_dumper, NULL) != 0) {
		close(st_dump_pipe[0]);
		close(st_dump_pipe[1]);
		st_dump_pipe[0] = st_dump_pipe[1] = -1;
	}
	pthread_attr_destroy(&a);
}

/* fork() child (2026-10-30, GAPS B87): the dumper thread does not survive fork, so a
 * SIGUSR2 to a forked DataLoader worker poked a pipe nobody read and no trace was
 * written. The child gets its own pipe and dumper (the trace path's %p is its pid). */
static void st_dumper_after_fork(void)
{
	if (st_dump_pipe[0] < 0)
		return;
	close(st_dump_pipe[0]);
	close(st_dump_pipe[1]);
	st_dump_pipe[0] = st_dump_pipe[1] = -1;
	st_start_dumper();
}

static void st_on_sigusr2(int sig)
{
	(void)sig;
	/* Safe no-op if tracing never armed — must not terminate the process. */
	if (st_enabled == 1 && st_inited) {
		int saved = errno;
		if (st_dump_pipe[1] >= 0)
			(void)!write(st_dump_pipe[1], "d", 1);
		else
			(void)lithe_sched_trace_dump();
		errno = saved;
	}
}

static void st_atexit_dump(void)
{
	if (st_enabled == 1)
		(void)lithe_sched_trace_dump();
}

static void st_init_once(void);

/* Install SIGUSR2 early when LITHE_SCHED_TRACE is set so a launcher's
 * kill -USR2 cannot hit the default terminate disposition (exit 140)
 * before the first FJS emit. Defer ring alloc to st_init_once (needs
 * max_vcores). Lithified exits often skip atexit via _exit. */
static void st_ctor_early_arm(void) __attribute__((constructor(101)));
static void st_ctor_early_arm(void)
{
	const char *e = getenv("LITHE_SCHED_TRACE");
	struct sigaction sa;

	if (!(e && *e && atoi(e)))
		return;
	st_enabled = 1;
	if (st_sigusr2_installed)
		return;
	memset(&sa, 0, sizeof(sa));
	sa.sa_handler = st_on_sigusr2;
	sigemptyset(&sa.sa_mask);
	sa.sa_flags = SA_RESTART;
	if (sigaction(SIGUSR2, &sa, &st_old_sigusr2) == 0)
		st_sigusr2_installed = 1;
}

/* parlib reactor -> trace ring bridge (installed in st_init_once). */
static void st_reactor_trace(int ev, int fd, int aux)
{
	static const uint8_t map[3] = { LITHE_ST_REACTOR_PARK, LITHE_ST_REACTOR_WAKE,
	                                LITHE_ST_REACTOR_EXPIRE };
	if (ev < 0 || ev > 2)
		return;
	lithe_sched_trace_emit(map[ev], NULL, (uint32_t)fd, 0, aux);
}

static int st_format(void) /* 0 bin, 1 perfetto, 2 both */
{
	static int cached = -1;
	if (cached < 0) {
		const char *e = getenv("LITHE_SCHED_TRACE_FORMAT");
		cached = 0;
		if (e && (!strcmp(e, "perfetto") || !strcmp(e, "proto")))
			cached = 1;
		else if (e && !strcmp(e, "both"))
			cached = 2;
	}
	return cached;
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
		else if (end != e && v > (1ul << 20)) {
			/* Used to fall back to the 4096 default silently, so asking for a
			 * bigger ring gave a 256x smaller one (2026-10-19). Clamp instead. */
			cap = 1u << 20;
			fprintf(stderr, "[LITHE_SCHED_TRACE] LITHE_SCHED_TRACE_RING=%lu above "
			        "max %u; using %u\n", v, cap, cap);
		}
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
	parlib_reactor_set_trace_fn(st_reactor_trace);
	st_start_dumper();
	pthread_atfork(NULL, NULL, st_dumper_after_fork);

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
	}
	/* ctor may set st_enabled=1 before rings exist */
	if (st_enabled == 1 && !st_inited)
		st_init_once();
	return st_enabled == 1;
}

/* Context labels and phase names (2026-10-24). Fixed tables, no malloc on the emit path:
 * labels by context id (open addressing, first writer wins), names by intern id. */
#define ST_LABELS 4096
#define ST_LABEL_LEN 24
/* rank: lithe_context_rank_self (2026-10-27), -1 when unset. Labels and ranks are also
 * kept when tracing is off but a profiling tool registered with parlib (parlib/tool.h),
 * which reads them through lithe_tool_describe. */
static struct { uint32_t key; int rank; char s[ST_LABEL_LEN]; } st_labels[ST_LABELS];
static int st_labels_used[ST_LABELS];
#define ST_NAMES 64
static char st_names[ST_NAMES][32];
static int st_n_names;
static int st_names_lock;

/* The entry for context id k, claimed if absent; NULL when the table is full. */
static int st_label_slot(uint32_t k)
{
	unsigned h = (k * 2654435761u) & (ST_LABELS - 1), i;
	for (i = 0; i < ST_LABELS; i++, h = (h + 1) & (ST_LABELS - 1)) {
		int z = 0;
		int u = __atomic_load_n(&st_labels_used[h], __ATOMIC_ACQUIRE);
		if (u == 2 && st_labels[h].key == k)
			return (int)h;
		if (u == 0 && __atomic_compare_exchange_n(&st_labels_used[h], &z, 1, 0, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)) {
			st_labels[h].key = k;
			st_labels[h].rank = -1;
			st_labels[h].s[0] = 0;
			__atomic_store_n(&st_labels_used[h], 2, __ATOMIC_RELEASE);
			return (int)h;
		}
		while (__atomic_load_n(&st_labels_used[h], __ATOMIC_ACQUIRE) == 1)
			;   /* another writer is filling this slot (a few stores) */
		if (st_labels[h].key == k)
			return (int)h;
	}
	return -1;
}

static int st_labels_wanted(void)
{
	return lithe_sched_trace_enabled() || parlib_tool_active();
}

void lithe_sched_trace_label_self(const char *label)
{
	int h;
	if (!label || !st_labels_wanted())
		return;
	h = st_label_slot(lithe_sched_trace_ctx_key());
	if (h >= 0 && !st_labels[h].s[0])   /* first writer wins */
		strncpy(st_labels[h].s, label, ST_LABEL_LEN - 1);
}

void lithe_context_rank_self(int rank)
{
	int h;
	if (!st_labels_wanted())
		return;
	h = st_label_slot(lithe_sched_trace_ctx_key());
	if (h >= 0)
		st_labels[h].rank = rank;
}

/* parlib tool describer (parlib/tool.h): context id, rank, label. Called from vcore
 * context by the tool; reads only (never claims a slot). */
int lithe_tool_describe(struct uthread *u, struct parlib_tool_uthread_info *out)
{
	lithe_context_t *c = (lithe_context_t *)u;   /* uth is the first member */
	unsigned h, i;
	out->id = c ? c->id : -1;
	out->rank = -1;
	out->label[0] = 0;
	if (!c)
		return 0;
	h = ((uint32_t)c->id * 2654435761u) & (ST_LABELS - 1);
	for (i = 0; i < ST_LABELS; i++, h = (h + 1) & (ST_LABELS - 1)) {
		int s = __atomic_load_n(&st_labels_used[h], __ATOMIC_ACQUIRE);
		if (s == 0)
			break;
		if (s == 2 && st_labels[h].key == (uint32_t)c->id) {
			out->rank = st_labels[h].rank;
			strncpy(out->label, st_labels[h].s, sizeof(out->label) - 1);
			out->label[sizeof(out->label) - 1] = 0;
			break;
		}
	}
	return 0;
}

static const char *st_label_of(uint32_t k)
{
	unsigned h = (k * 2654435761u) & (ST_LABELS - 1), i;
	for (i = 0; i < ST_LABELS; i++, h = (h + 1) & (ST_LABELS - 1)) {
		int u = __atomic_load_n(&st_labels_used[h], __ATOMIC_ACQUIRE);
		if (u == 0)
			return NULL;
		if (u == 2 && st_labels[h].key == k)
			return st_labels[h].s;
	}
	return NULL;
}

int32_t lithe_sched_trace_intern(const char *name)
{
	int i, id = 0;
	if (!lithe_sched_trace_enabled() || !name)
		return 0;
	while (__atomic_exchange_n(&st_names_lock, 1, __ATOMIC_ACQUIRE))
		;
	for (i = 0; i < st_n_names; i++)
		if (!strncmp(st_names[i], name, sizeof(st_names[i]) - 1)) { id = i + 1; break; }
	if (!id && st_n_names < ST_NAMES) {
		strncpy(st_names[st_n_names], name, sizeof(st_names[0]) - 1);
		id = ++st_n_names;
	}
	__atomic_store_n(&st_names_lock, 0, __ATOMIC_RELEASE);
	return id;
}

static const char *st_name_of(int32_t id)
{
	return (id >= 1 && id <= st_n_names) ? st_names[id - 1] : "phase";
}

void lithe_sched_trace_phase_begin(int32_t id)
{
	if (lithe_sched_trace_enabled())
		lithe_sched_trace_emit(LITHE_ST_PHASE_BEGIN, NULL, lithe_sched_trace_ctx_key(), 0, id);
}

void lithe_sched_trace_phase_end(void)
{
	if (lithe_sched_trace_enabled())
		lithe_sched_trace_emit(LITHE_ST_PHASE_END, NULL, lithe_sched_trace_ctx_key(), 0, 0);
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
	case LITHE_ST_OMP_FORK_BEGIN: return "omp_fork_begin";
	case LITHE_ST_OMP_FORK_END: return "omp_fork_end";
	case LITHE_ST_OMP_RUN_BEGIN: return "omp_run_begin";
	case LITHE_ST_OMP_RUN_END: return "omp_run_end";
	case LITHE_ST_OMP_BARRIER_BEGIN: return "omp_barrier_begin";
	case LITHE_ST_OMP_BARRIER_END: return "omp_barrier_end";
	case LITHE_ST_REACTOR_PARK: return "reactor_park";
	case LITHE_ST_REACTOR_WAKE: return "reactor_wake";
	case LITHE_ST_REACTOR_EXPIRE: return "reactor_expire";
	case LITHE_ST_WAIT_SYNC_BLOCK: return "wait_sync_block";
	case LITHE_ST_WAIT_SYNC_WAKE: return "wait_sync_wake";
	case LITHE_ST_USER_BEGIN: return "user_begin";
	case LITHE_ST_USER_END: return "user_end";
	case LITHE_ST_WARM_PARK: return "warm_park";
	case LITHE_ST_HART_REQUEST: return "hart_request";
	case LITHE_ST_BASE_REQUEST: return "base_hart_request";
	case LITHE_ST_BASE_GRANT: return "base_grant";
	case LITHE_ST_PHASE_BEGIN: return "phase_begin";
	case LITHE_ST_PHASE_END: return "phase_end";
	case LITHE_ST_TCM_PERMIT: return "tcm_permit";
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
	/* LITHE_SCHED_TRACE_SKIP_IDLE=1 (2026-10-19): idle harts emit >1M idle_spin
	 * records/s and wrap the ring, so a busy hart's history is lost ([dropped]
	 * in the report). Skipping them keeps whole runs; idle ms then reads 0. */
	if (ev == LITHE_ST_IDLE_SPIN) {
		static int skip = -1;
		if (skip < 0) {
			const char *e = getenv("LITHE_SCHED_TRACE_SKIP_IDLE");
			skip = e && e[0] == '1';
		}
		if (skip)
			return;
	}

	vc = vcore_id();
	if (vc < 0 || (unsigned)vc >= st_n_vcores)
		return; /* off-vcore callers must not race vcore 0 ring */
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

static int st_dump_bin(const char *path)
{
	int fd;
	unsigned int i;
	lithe_sched_trace_file_hdr_t hdr;
	lithe_sched_trace_counters_t ctrs;
	ssize_t n;

	/* Finalize-wrap / SIGUSR2 may run before any emit; arm from env. */
	if (!lithe_sched_trace_enabled() || !st_inited)
		return -1;
	fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
	if (fd < 0) {
		fprintf(stderr, "[LITHE_SCHED_TRACE] open(%s) failed: %s\n",
		        path, strerror(errno));
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
			/* The ring is contiguous in at most two pieces: [first .. cap)
			 * and [0 .. first). Write each piece in large chunks. The old
			 * one-record-per-write() loop issued ~10 M syscalls for a full
			 * 9-vcore ring and sat 15+ minutes in exit on Lustre
			 * (2026-10-16, BabelStream C10 test). */
			uint64_t first = start & st_ring_mask;
			uint64_t piece1 = (first + nrec <= st_ring_cap) ? nrec
			                                                : (st_ring_cap - first);
			uint64_t piece2 = nrec - piece1;
			const char *p1 = (const char *)&st_rings[i][first];
			const char *p2 = (const char *)&st_rings[i][0];
			size_t left1 = piece1 * sizeof(lithe_sched_trace_rec_t);
			size_t left2 = piece2 * sizeof(lithe_sched_trace_rec_t);
			while (left1 > 0) {
				size_t chunk = left1 > (1u << 22) ? (1u << 22) : left1;
				n = write(fd, p1, chunk);
				if (n <= 0)
					goto fail;
				p1 += n; left1 -= (size_t)n;
			}
			while (left2 > 0) {
				size_t chunk = left2 > (1u << 22) ? (1u << 22) : left2;
				n = write(fd, p2, chunk);
				if (n <= 0)
					goto fail;
				p2 += n; left2 -= (size_t)n;
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
	        path,
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

/* ======================================================================
 * Native Perfetto protobuf writer (LITHE_SCHED_TRACE_FORMAT=perfetto|both)
 * Hand-rolled wire encoding of the few TracePacket / TrackDescriptor /
 * TrackEvent fields we need; no protobuf library. Field numbers are from
 * perfetto/protos/perfetto/trace/{trace_packet,track_event}/… .proto files.
 * Model: process = PID; thread track per hart (slices "ctx N" / "-- idle --",
 * instants for scheduler events and flagged decisions); async counter track
 * per hart (runnable_count); async track per OpenMP thread ("parallel
 * region" / "run" / "barrier" slices from libomp); async track per context
 * ("MPI wait (wait_sync)" from OMPI, "user N" from the app); flows from
 * context_unblock to that context's next dispatch; a ClockSnapshot so the
 * trace merges with other MONOTONIC_RAW/BOOTTIME sources.
 * ====================================================================== */
#define PB_VARINT 0
#define PB_FIXED64 1
#define PB_LEN 2

typedef struct { uint8_t *b; size_t n, cap; int fd; int err; } pb_out_t;
typedef struct { uint8_t b[1024]; size_t n; int ovf; } pb_msg_t;

static size_t pb_enc_varint(uint8_t *d, uint64_t v)
{
	size_t i = 0;
	while (v >= 0x80) { d[i++] = (uint8_t)(v | 0x80); v >>= 7; }
	d[i++] = (uint8_t)v;
	return i;
}
static void pbo_flush(pb_out_t *o)
{
	size_t off = 0;
	while (off < o->n && !o->err) {
		ssize_t w = write(o->fd, o->b + off, o->n - off);
		if (w <= 0) { o->err = errno ? errno : EIO; break; }
		off += (size_t)w;
	}
	o->n = 0;
}
static void pbo_put(pb_out_t *o, const void *p, size_t n)
{
	if (o->n + n > o->cap) pbo_flush(o);
	if (n > o->cap) { o->err = EFBIG; return; }
	memcpy(o->b + o->n, p, n); o->n += n;
}
static void pbm_init(pb_msg_t *m) { m->n = 0; m->ovf = 0; }
static void pbm_varint(pb_msg_t *m, uint64_t v)
{
	if (m->n + 10 > sizeof(m->b)) { m->ovf = 1; return; }
	m->n += pb_enc_varint(m->b + m->n, v);
}
static void pbm_tag(pb_msg_t *m, uint32_t f, int wt) { pbm_varint(m, ((uint64_t)f << 3) | (uint64_t)wt); }
static void pbm_u64(pb_msg_t *m, uint32_t f, uint64_t v) { pbm_tag(m, f, PB_VARINT); pbm_varint(m, v); }
static void pbm_i64(pb_msg_t *m, uint32_t f, int64_t v) { pbm_u64(m, f, (uint64_t)v); }
static void pbm_fixed64(pb_msg_t *m, uint32_t f, uint64_t v)
{
	pbm_tag(m, f, PB_FIXED64);
	if (m->n + 8 > sizeof(m->b)) { m->ovf = 1; return; }
	memcpy(m->b + m->n, &v, 8); m->n += 8;
}
static void pbm_bytes(pb_msg_t *m, uint32_t f, const void *p, size_t n)
{
	pbm_tag(m, f, PB_LEN); pbm_varint(m, n);
	if (m->n + n > sizeof(m->b)) { m->ovf = 1; return; }
	memcpy(m->b + m->n, p, n); m->n += n;
}
static void pbm_str(pb_msg_t *m, uint32_t f, const char *s) { pbm_bytes(m, f, s, strlen(s)); }
static void pbm_msg(pb_msg_t *m, uint32_t f, const pb_msg_t *sub) { pbm_bytes(m, f, sub->b, sub->n); }
static void pbo_packet(pb_out_t *o, pb_msg_t *pkt)
{
	uint8_t hdr[16]; size_t h = 0;
	if (pkt->ovf) { o->err = EMSGSIZE; return; }
	h += pb_enc_varint(hdr, (1u << 3) | PB_LEN);   /* Trace.packet = 1 */
	h += pb_enc_varint(hdr + h, pkt->n);
	pbo_put(o, hdr, h); pbo_put(o, pkt->b, pkt->n);
}

#define TP_TIMESTAMP 8
#define TP_TRUSTED_SEQ 10
#define TP_TRACK_EVENT 11
#define TP_SEQ_FLAGS 13
#define TP_TRACK_DESCRIPTOR 60
#define TP_CLOCK_ID 58
#define TP_CLOCK_SNAPSHOT 6
#define TD_UUID 1
#define TD_NAME 2
#define TD_PROCESS 3
#define TD_THREAD 4
#define TD_PARENT 5
#define TD_COUNTER 8
#define TE_DEBUG_ANN 4
#define TE_TYPE 9
#define TE_TRACK_UUID 11
#define TE_CATEGORIES 22
#define TE_NAME 23
#define TE_COUNTER_VALUE 30
#define TE_FLOW_IDS 47
#define TE_TERM_FLOW_IDS 48
#define TE_SLICE_BEGIN 1
#define TE_SLICE_END 2
#define TE_INSTANT 3
#define TE_COUNTER 4
#define DA_INT 4
#define DA_NAME 10

typedef struct { const char *name; int64_t val; } pf_ann_t;

static int pf_first_packet = 1;
static int pf_clock_id = 6;

static uint64_t pf_seq_id = 1;      /* per process: concatenated files must not share a sequence */
static int64_t pf_boot_off = 0;     /* BOOTTIME - record clock, so timestamps need no cross-file clock sync */
static void pf_seq(pb_msg_t *p)
{
	pbm_u64(p, TP_TRUSTED_SEQ, pf_seq_id);
	if (pf_first_packet) { pbm_u64(p, TP_SEQ_FLAGS, 1 /* SEQ_INCREMENTAL_STATE_CLEARED */); pf_first_packet = 0; }
}

static void pf_event(pb_out_t *o, uint64_t ts, uint64_t track, int type,
                     const char *name, const char *cat,
                     const int64_t *counter, uint64_t flow, uint64_t term_flow,
                     const pf_ann_t *ann, int n_ann)
{
	pb_msg_t pkt, te; int i;
	pbm_init(&pkt); pbm_init(&te);
	pbm_u64(&pkt, TP_TIMESTAMP, ts);
	pbm_u64(&pkt, TP_CLOCK_ID, (uint64_t)pf_clock_id);
	pf_seq(&pkt);
	pbm_u64(&te, TE_TYPE, (uint64_t)type);
	pbm_u64(&te, TE_TRACK_UUID, track);
	if (name) pbm_str(&te, TE_NAME, name);
	if (cat) pbm_str(&te, TE_CATEGORIES, cat);
	if (counter) pbm_i64(&te, TE_COUNTER_VALUE, *counter);
	if (flow) pbm_fixed64(&te, TE_FLOW_IDS, flow);
	if (term_flow) pbm_fixed64(&te, TE_TERM_FLOW_IDS, term_flow);
	for (i = 0; i < n_ann; i++) {
		pb_msg_t da; pbm_init(&da);
		pbm_str(&da, DA_NAME, ann[i].name);
		pbm_i64(&da, DA_INT, ann[i].val);
		pbm_msg(&te, TE_DEBUG_ANN, &da);
	}
	pbm_msg(&pkt, TP_TRACK_EVENT, &te);
	pbo_packet(o, &pkt);
}
static void pf_track_thread(pb_out_t *o, uint64_t uuid, int pid, int tid, const char *name)
{
	pb_msg_t pkt, td, th; pbm_init(&pkt); pbm_init(&td); pbm_init(&th);
	pf_seq(&pkt);
	pbm_u64(&td, TD_UUID, uuid);
	pbm_i64(&th, 1, pid); pbm_i64(&th, 2, tid); pbm_str(&th, 5, name);
	pbm_msg(&td, TD_THREAD, &th);
	pbm_msg(&pkt, TP_TRACK_DESCRIPTOR, &td);
	pbo_packet(o, &pkt);
}
static void pf_track_process(pb_out_t *o, uint64_t uuid, int pid, const char *name)
{
	pb_msg_t pkt, td, pr; pbm_init(&pkt); pbm_init(&td); pbm_init(&pr);
	pf_seq(&pkt);
	pbm_u64(&td, TD_UUID, uuid);
	pbm_i64(&pr, 1, pid); pbm_str(&pr, 6, name);
	pbm_msg(&td, TD_PROCESS, &pr);
	pbm_msg(&pkt, TP_TRACK_DESCRIPTOR, &td);
	pbo_packet(o, &pkt);
}
static void pf_track_async(pb_out_t *o, uint64_t uuid, uint64_t parent, const char *name, int counter)
{
	pb_msg_t pkt, td; pbm_init(&pkt); pbm_init(&td);
	pf_seq(&pkt);
	pbm_u64(&td, TD_UUID, uuid);
	pbm_u64(&td, TD_PARENT, parent);
	pbm_str(&td, TD_NAME, name);
	if (counter) { pb_msg_t cd; pbm_init(&cd); pbm_msg(&td, TD_COUNTER, &cd); }
	pbm_msg(&pkt, TP_TRACK_DESCRIPTOR, &td);
	pbo_packet(o, &pkt);
}
static void pf_clock_snapshot(pb_out_t *o)
{
	struct timespec raw, mono, boot, real;
	pb_msg_t pkt, cs, c; int have_raw = 0;
	pbm_init(&pkt); pbm_init(&cs);
	pf_seq(&pkt);
#if defined(CLOCK_MONOTONIC_RAW)
	have_raw = (clock_gettime(CLOCK_MONOTONIC_RAW, &raw) == 0);
#endif
	clock_gettime(CLOCK_MONOTONIC, &mono);
	clock_gettime(CLOCK_BOOTTIME, &boot);
	clock_gettime(CLOCK_REALTIME, &real);
#define PF_CLK(id, ts) do { pbm_init(&c); pbm_u64(&c, 1, (id)); \
	pbm_u64(&c, 2, (uint64_t)(ts).tv_sec * 1000000000ull + (uint64_t)(ts).tv_nsec); \
	pbm_msg(&cs, 1, &c); } while (0)
	if (have_raw) PF_CLK(5, raw);
	PF_CLK(3, mono); PF_CLK(6, boot); PF_CLK(1, real);
#undef PF_CLK
	/* Records are stamped with the st_now_ns() clock (MONOTONIC_RAW when
	 * available, else MONOTONIC). Emit every event in BOOTTIME instead:
	 * the trace processor rejects a ClockSnapshot that is older than the
	 * previous one, so two concatenated per-rank files whose ranks dumped
	 * in the "wrong" order lost every event of the second rank
	 * (clock_sync_failure, 2026-10-17). With BOOTTIME timestamps the
	 * snapshot is informational only (merging with other traces). */
	{
		struct timespec *base = have_raw ? &raw : &mono;
		int64_t b = (int64_t)boot.tv_sec * 1000000000ll + boot.tv_nsec;
		int64_t r = (int64_t)base->tv_sec * 1000000000ll + base->tv_nsec;
		pf_boot_off = b - r;
	}
	pbm_u64(&cs, 2, 6); /* primary_trace_clock = BOOTTIME */
	pbm_msg(&pkt, TP_CLOCK_SNAPSHOT, &cs);
	pbo_packet(o, &pkt);
}

#define PF_SET_SZ 8192
/* Wake flows (2026-10-24). The writer emits one hart's records after another, so
 * matching an unblock to "the context's next dispatch" while emitting picked the next
 * one in *processing* order: an unblock on hart 2 whose context ran on hart 1 a
 * microsecond later (already emitted) was tied to some much later dispatch. The
 * report's wake -> dispatch latency read p90 1.6-2.2 ms for HPX workers whose true
 * p90 is 1 us, and 300 us for OpenMP threads whose true p90 is 68 us (one chunk).
 * This pass pairs, per context and in time across all harts, each dispatch with the
 * latest unblock since the previous dispatch; the emit pass reads the flow ids back
 * in record order. */
/* The records a perfetto dump walks on one hart (2026-10-30). Harts keep recording while the
 * dump runs, so once a ring has wrapped its oldest slots are being overwritten under the
 * reader: pf_match_wakes counted one set of records and filled another -- more matches than
 * it had allocated (heap overflow) or fewer (qsort over uninitialised pointers). Every
 * GROMACS 2000-step trace segfaulted at exit and left 0-byte files
 * (.build_logs/gmx_gompl_gap/core_run). A wrapped ring's oldest quarter is left out, so the
 * passes read slots the writers will not reach during the dump. */
static void pf_window(uint64_t seq, uint64_t *start, uint64_t *nrec)
{
	if (seq <= st_ring_cap) { *nrec = seq; *start = 0; return; }
	*nrec = st_ring_cap - st_ring_cap / 4;
	*start = seq - *nrec;
}

typedef struct { uint64_t ns, g, fid; uint32_t ctx; uint8_t disp; } pf_wake_t;
static int pf_wake_cmp_ctx_time(const void *a, const void *b)
{
	const pf_wake_t *x = *(pf_wake_t *const *)a, *y = *(pf_wake_t *const *)b;
	if (x->ctx != y->ctx) return x->ctx < y->ctx ? -1 : 1;
	if (x->ns != y->ns) return x->ns < y->ns ? -1 : 1;
	if (x->disp != y->disp) return x->disp ? 1 : -1; /* unblock before a same-ns dispatch */
	return x->g < y->g ? -1 : x->g > y->g;
}
static int pf_is_dispatch(uint8_t ev)
{
	return ev == LITHE_ST_DEQUEUE_LOCAL || ev == LITHE_ST_STEAL_SUCCESS || ev == LITHE_ST_WARM_CLAIM;
}
/* Returns the events (record order) with fid set on matched pairs, *n their count;
 * NULL (flows omitted) when there are none or memory is short. seqs: each hart's
 * write sequence, snapshotted once -- harts still record while the dump runs, and both
 * passes must walk the same records. */
static pf_wake_t *pf_match_wakes(const uint64_t *seqs, uint32_t pid, uint64_t *flow_seq, size_t *n)
{
	size_t cnt = 0, j = 0, c;
	uint64_t i, k, g = 0;
	pf_wake_t *w, **ix;
	*n = 0;
	for (i = 0; i < st_n_vcores; i++) {
		uint64_t seq = seqs[i], nrec, start;
		if (seq == 0 || !st_rings[i]) continue;
		pf_window(seq, &start, &nrec);
		for (k = 0; k < nrec; k++) {
			uint8_t ev = st_rings[i][(start + k) & st_ring_mask].ev;
			cnt += ev == LITHE_ST_CTX_UNBLOCK || pf_is_dispatch(ev);
		}
	}
	if (cnt == 0 || !(w = calloc(cnt, sizeof(*w))))
		return NULL;
	if (!(ix = malloc(cnt * sizeof(*ix)))) {
		free(w);
		return NULL;
	}
	for (i = 0; i < st_n_vcores; i++) {
		uint64_t seq = seqs[i], nrec, start;
		if (seq == 0 || !st_rings[i]) continue;
		pf_window(seq, &start, &nrec);
		for (k = 0; k < nrec; k++, g++) {
			const lithe_sched_trace_rec_t *r = &st_rings[i][(start + k) & st_ring_mask];
			int d = pf_is_dispatch(r->ev);
			if (!d && r->ev != LITHE_ST_CTX_UNBLOCK) continue;
			if (j == cnt) break; /* the record changed since the count: never overrun */
			w[j].ns = r->ns; w[j].g = g; w[j].ctx = r->ctx_id; w[j].disp = (uint8_t)d;
			ix[j] = &w[j]; j++;
		}
	}
	cnt = j; /* and never sort slots the fill did not reach */
	qsort(ix, cnt, sizeof(*ix), pf_wake_cmp_ctx_time);
	for (c = 0; c < cnt; ) {
		pf_wake_t *pend = NULL;
		uint32_t ctx = ix[c]->ctx;
		for (; c < cnt && ix[c]->ctx == ctx; c++) {
			if (!ix[c]->disp) { pend = ix[c]; continue; }
			if (pend) {
				uint64_t fid = ((uint64_t)pid << 32) | ((*flow_seq)++ & 0xffffffffu);
				pend->fid = ix[c]->fid = fid;
				pend = NULL;
			}
		}
	}
	free(ix);
	*n = cnt;
	return w;
}

typedef struct { uint64_t key[PF_SET_SZ]; uint64_t val[PF_SET_SZ]; uint8_t used[PF_SET_SZ]; } pf_map_t;
static uint64_t *pf_map_get(pf_map_t *m, uint64_t k, int *is_new)
{
	unsigned h = (unsigned)((k * 0x9E3779B97F4A7C15ull) >> 51) & (PF_SET_SZ - 1);
	unsigned i;
	*is_new = 0;
	for (i = 0; i < PF_SET_SZ; i++) {
		unsigned j = (h + i) & (PF_SET_SZ - 1);
		if (!m->used[j]) { m->used[j] = 1; m->key[j] = k; m->val[j] = 0; *is_new = 1; return &m->val[j]; }
		if (m->key[j] == k) return &m->val[j];
	}
	return NULL;
}

int lithe_sched_trace_dump_perfetto(const char *path)
{
	int fd, pid = (int)getpid();
	unsigned int i;
	pb_out_t o;
	uint64_t P;
	pf_map_t *omp_tracks, *ctx_tracks, *tcm_tracks;
	uint64_t flow_seq = 1, total = 0, g = 0;
	const char *iie = getenv("LITHE_SCHED_TRACE_IDLE_INSTANTS");
	int idle_instants = iie && iie[0] == '1';
	pf_wake_t *wakes = NULL;
	static uint64_t seqs[LITHE_SCHED_TRACE_MAX_VCORES];
	size_t nwakes = 0, wi = 0;
	char name[96];

	if (!lithe_sched_trace_enabled() || !st_inited)
		return -1;
	fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
	if (fd < 0) {
		fprintf(stderr, "[LITHE_SCHED_TRACE] open(%s) failed: %s\n", path, strerror(errno));
		return -1;
	}
	o.cap = 4u << 20; o.b = malloc(o.cap); o.n = 0; o.fd = fd; o.err = 0;
	omp_tracks = calloc(1, sizeof(*omp_tracks));
	ctx_tracks = calloc(1, sizeof(*ctx_tracks));
	tcm_tracks = calloc(1, sizeof(*tcm_tracks));
	if (!o.b || !omp_tracks || !ctx_tracks || !tcm_tracks) {
		close(fd); free(o.b); free(omp_tracks); free(ctx_tracks); free(tcm_tracks);
		return -1;
	}
	pf_clock_id = 6; /* BOOTTIME, see pf_clock_snapshot */
	pf_seq_id = 2 + ((uint64_t)pid & 0xffffff);
	pf_first_packet = 1;
	P = (uint64_t)pid * 1000003ull + 1;
	pf_clock_snapshot(&o);
	{
		/* Label the process with its launcher rank when one is set, so a
		 * concatenated multi-rank trace reads "rank 1" rather than a PID. */
		const char *rk = getenv("OMPI_COMM_WORLD_RANK");
		if (!rk || !*rk) rk = getenv("PMI_RANK");
		if (!rk || !*rk) rk = getenv("FLUX_TASK_RANK");
		if (!rk || !*rk) rk = getenv("SLURM_PROCID");
		if (rk && *rk)
			snprintf(name, sizeof(name), "rank %s pid %d", rk, pid);
		else
			snprintf(name, sizeof(name), "lithe pid %d", pid);
	}
	pf_track_process(&o, P, pid, name);
	for (i = 0; i < st_n_vcores; i++)
		seqs[i] = __atomic_load_n(&st_write_seq[i], __ATOMIC_ACQUIRE);
	for (i = 0; i < st_n_vcores; i++) {
		int cpu;
		if (seqs[i] == 0) continue;
		/* The CPU parlib pinned this vcore to (-1 when unpinned / kernel-balanced),
		 * so a placement artifact is visible in the trace itself (PLAN-affinity A7). */
		cpu = parlib_vcore_cpu((int)i);
		if (cpu >= 0 && parlib_vcore_pinned((int)i))
			snprintf(name, sizeof(name), "hart %u (cpu %d)%s", i, cpu, st_dropped[i] ? " [dropped]" : "");
		else if (cpu >= 0)
			snprintf(name, sizeof(name), "hart %u (unpinned, started cpu %d)%s", i, cpu, st_dropped[i] ? " [dropped]" : "");
		else
			snprintf(name, sizeof(name), "hart %u%s", i, st_dropped[i] ? " [dropped]" : "");
		pf_track_thread(&o, P + 100 + i, pid, (int)i + 1, name);
		snprintf(name, sizeof(name), "runnable hart %u", i);
		pf_track_async(&o, P + 300 + i, P, name, 1);
	}

	wakes = pf_match_wakes(seqs, (uint32_t)pid, &flow_seq, &nwakes);
	for (i = 0; i < st_n_vcores && !o.err; i++) {
		uint64_t seq = seqs[i], nrec, start, k;
		uint64_t hart = P + 100 + i, ctr = P + 300 + i;
		int cur = 0; /* 0 none, -1 idle, >0 ctx */
		uint64_t last_ts = 0; /* last slice begin/end on this hart track */
		if (seq == 0 || !st_rings[i]) continue;
		pf_window(seq, &start, &nrec);
		total += nrec;
		for (k = 0; k < nrec && !o.err; k++, g++) {
			const lithe_sched_trace_rec_t *r = &st_rings[i][(start + k) & st_ring_mask];
			uint64_t ts = (uint64_t)((int64_t)r->ns + pf_boot_off);
			/* this record's wake flow id from pf_match_wakes (0: none) */
			uint64_t wfid = 0;
			if (wakes && wi < nwakes && wakes[wi].g == g)
				wfid = wakes[wi++].fid;
			/* Equal timestamps on one thread track (yield -> idle begin, or an
			 * instant on the ts of a slice end) are order-ambiguous for the trace
			 * processor and showed up as misplaced_end_event; keep every slice
			 * begin/end on a hart track strictly after the previous one. */
#define HART_SLICE(type, name, cat, term, ann, na) do { \
				if (ts <= last_ts) ts = last_ts + 1; \
				pf_event(&o, ts, hart, (type), (name), (cat), NULL, 0, (term), (ann), (na)); \
				last_ts = ts; } while (0)
			/* Instants on the hart track too (2026-10-24): an instant left at the raw ts
			 * tied with the next slice begin, and the trace processor nested that slice
			 * inside the zero-length instant -- 3637 misplaced_end_event and zero-length
			 * "-- idle --" slices in the TBB+OpenMP mix. Every event on a hart track now
			 * gets a strictly increasing timestamp. */
#define HART_EV(type, name, cat, flow, ann, na) do { \
				if (ts <= last_ts) ts = last_ts + 1; \
				pf_event(&o, ts, hart, (type), (name), (cat), NULL, (flow), 0, (ann), (na)); \
				last_ts = ts; } while (0)
			const char *evn = lithe_sched_trace_ev_name(r->ev);
			pf_ann_t ann[3];
			/* Hart life-cycle instants (2026-10-17): a hart that leaves the
			 * scheduler used to vanish from the trace with no event, and a
			 * request the base scheduler refused was invisible (XSBench OMP=4:
			 * two woken workers waited 136 ms for harts that never came). */
			switch (r->ev) {
			case LITHE_ST_HART_RETURN: case LITHE_ST_IDLE_YIELD: case LITHE_ST_HART_ENTER:
			case LITHE_ST_HART_REQUEST: case LITHE_ST_BASE_REQUEST: case LITHE_ST_BASE_GRANT:
			case LITHE_ST_CHILD_EXIT: case LITHE_ST_WARM_PARK: {
				pf_ann_t a2[4];
				a2[0].name = "ctx"; a2[0].val = r->ctx_id; a2[1].name = "aux"; a2[1].val = r->aux;
				a2[2].name = "dec"; a2[2].val = r->decision; a2[3].name = "sched"; a2[3].val = r->sched_id;
				HART_EV(TE_INSTANT, evn, "hart", 0, a2, 4);
				break; }
			default: break;
			}
			if ((r->ev == LITHE_ST_DEQUEUE_LOCAL || r->ev == LITHE_ST_HART_ENTER ||
			     r->ev == LITHE_ST_IDLE_YIELD || r->ev == LITHE_ST_CTX_YIELD) && r->aux >= 0) {
				int64_t v = r->aux;
				pf_event(&o, ts, ctr, TE_COUNTER, NULL, NULL, &v, 0, 0, NULL, 0);
			}
			switch (r->ev) {
			case LITHE_ST_DEQUEUE_LOCAL: case LITHE_ST_STEAL_SUCCESS: case LITHE_ST_WARM_CLAIM: {
				/* A dispatch always carries a context. Context ids start at 0
				 * (lithe.c next_context_id), so "ctx 0" is the first context of
				 * the process: the MPI rank / OpenMP primary. Until 2026-10-17 it
				 * was skipped and the primary's time showed as "-- idle --". */
				uint64_t term = wfid;
				if (cur) HART_SLICE(TE_SLICE_END, NULL, NULL, 0, NULL, 0);
				{ const char *lb = st_label_of(r->ctx_id);
				  if (lb) snprintf(name, sizeof(name), "ctx %u %s", r->ctx_id, lb);
				  else snprintf(name, sizeof(name), "ctx %u", r->ctx_id); }
				ann[0].name = "ctx"; ann[0].val = r->ctx_id;
				ann[1].name = "sched"; ann[1].val = r->sched_id; /* which FJS dispatched it (nested scheds share a hart track) */
				HART_SLICE(TE_SLICE_BEGIN, name, "ctx", term, ann, 2);
				cur = (int)r->ctx_id + 1; /* 0 none, -1 idle, >0 ctx id + 1 */
				break; }
			case LITHE_ST_CTX_YIELD: case LITHE_ST_CTX_BLOCK: case LITHE_ST_HART_RETURN:
			case LITHE_ST_WARM_PARK: case LITHE_ST_CHILD_EXIT:
				if (cur) HART_SLICE(TE_SLICE_END, NULL, NULL, 0, NULL, 0);
				cur = 0;
				if (r->ev != LITHE_ST_HART_RETURN) {
					HART_SLICE(TE_SLICE_BEGIN, "-- idle --", "idle", 0, NULL, 0);
					cur = -1;
				}
				break;
			case LITHE_ST_IDLE_SPIN: case LITHE_ST_IDLE_YIELD: case LITHE_ST_REACTOR_BLOCK:
				if (cur != -1) {
					if (cur) HART_SLICE(TE_SLICE_END, NULL, NULL, 0, NULL, 0);
					HART_SLICE(TE_SLICE_BEGIN, "-- idle --", "idle", 0, NULL, 0);
					cur = -1;
				}
				/* LITHE_SCHED_TRACE_IDLE_INSTANTS=1: idle-spin exits as instants too (aux =
				 * which check fired), to see what an idle hart's loop does. Off by default:
				 * idle harts record >1M of them per second. */
				if (r->ev == LITHE_ST_IDLE_SPIN && idle_instants) {
					ann[0].name = "why"; ann[0].val = r->aux; ann[1].name = "sched"; ann[1].val = r->sched_id;
					HART_EV(TE_INSTANT, "idle_spin", "hart", 0, ann, 2);
				}
				break;
			case LITHE_ST_CTX_UNBLOCK: {
				ann[0].name = "ctx"; ann[0].val = r->ctx_id; ann[1].name = "aux"; ann[1].val = r->aux;
				HART_EV(TE_INSTANT, "context_unblock", "sched", wfid, ann, 2);
				break; }
			case LITHE_ST_STEAL_ATTEMPT: case LITHE_ST_STEAL_FAIL: case LITHE_ST_PROGRESS_WAKE:
			case LITHE_ST_HART_GRANT: case LITHE_ST_CHILD_ENTER:
				ann[0].name = "ctx"; ann[0].val = r->ctx_id; ann[1].name = "aux"; ann[1].val = r->aux;
				ann[2].name = "sched"; ann[2].val = r->sched_id;
				HART_EV(TE_INSTANT, evn, "sched", 0, ann, 3);
				break;
			case LITHE_ST_REACTOR_PARK: case LITHE_ST_REACTOR_WAKE: case LITHE_ST_REACTOR_EXPIRE:
				ann[0].name = "fd"; ann[0].val = r->ctx_id;
				ann[1].name = (r->ev == LITHE_ST_REACTOR_PARK) ? "timeout_ms" : "events"; ann[1].val = r->aux;
				HART_EV(TE_INSTANT, evn, "reactor", 0, ann, 2);
				break;
			case LITHE_ST_OMP_FORK_BEGIN: case LITHE_ST_OMP_FORK_END:
			case LITHE_ST_OMP_RUN_BEGIN: case LITHE_ST_OMP_RUN_END:
			case LITHE_ST_OMP_BARRIER_BEGIN: case LITHE_ST_OMP_BARRIER_END: {
				int is_new; uint64_t *t = pf_map_get(omp_tracks, r->ctx_id, &is_new);
				uint64_t track = P + 1000 + r->ctx_id;
				if (!t) break;
				if (is_new) { snprintf(name, sizeof(name), "omp thread %u", r->ctx_id); pf_track_async(&o, track, P, name, 0); }
				if (r->ev == LITHE_ST_OMP_FORK_BEGIN) {
					ann[0].name = "hart"; ann[0].val = r->vcore_id;
					pf_event(&o, ts, track, TE_SLICE_BEGIN, "parallel region", "omp", NULL, 0, 0, ann, 1);
				} else if (r->ev == LITHE_ST_OMP_RUN_BEGIN) {
					ann[0].name = "hart"; ann[0].val = r->vcore_id; ann[1].name = "tid"; ann[1].val = r->aux;
					pf_event(&o, ts, track, TE_SLICE_BEGIN, "run", "omp", NULL, 0, 0, ann, 2);
				} else if (r->ev == LITHE_ST_OMP_BARRIER_BEGIN) {
					ann[0].name = "hart"; ann[0].val = r->vcore_id; ann[1].name = "type"; ann[1].val = r->aux;
					pf_event(&o, ts, track, TE_SLICE_BEGIN,
					         r->aux == 1 ? "fork/join barrier" : (r->aux == 2 ? "reduction barrier" : "barrier"),
					         "omp", NULL, 0, 0, ann, 2);
				} else {
					pf_event(&o, ts, track, TE_SLICE_END, NULL, NULL, NULL, 0, 0, NULL, 0);
				}
				break; }
			case LITHE_ST_TCM_PERMIT: {
				int is_new; uint64_t *t = pf_map_get(tcm_tracks, r->ctx_id, &is_new);
				uint64_t track = P + 5000000ull + r->ctx_id;
				int64_t v = r->aux;
				if (!t) break;
				if (is_new) { snprintf(name, sizeof(name), "tcm permit %u (granted)", r->ctx_id); pf_track_async(&o, track, P, name, 1); }
				pf_event(&o, ts, track, TE_COUNTER, NULL, NULL, &v, 0, 0, NULL, 0);
				break; }
			case LITHE_ST_WAIT_SYNC_BLOCK: case LITHE_ST_WAIT_SYNC_WAKE:
			case LITHE_ST_PHASE_BEGIN: case LITHE_ST_PHASE_END:
			case LITHE_ST_USER_BEGIN: case LITHE_ST_USER_END: {
				int is_new; uint64_t *t = pf_map_get(ctx_tracks, r->ctx_id, &is_new);
				uint64_t track = P + 3000000ull + r->ctx_id;
				if (!t) break;
				if (is_new) {
					const char *lb = st_label_of(r->ctx_id);
					if (r->ctx_id & 0x80000000u) snprintf(name, sizeof(name), "context %#x", r->ctx_id);
					else if (lb) snprintf(name, sizeof(name), "context %u %s", r->ctx_id, lb);
					else snprintf(name, sizeof(name), "context %u", r->ctx_id);
					pf_track_async(&o, track, P, name, 0);
				}
				if (r->ev == LITHE_ST_WAIT_SYNC_BLOCK) {
					ann[0].name = "hart"; ann[0].val = r->vcore_id; ann[1].name = "sync"; ann[1].val = r->aux;
					pf_event(&o, ts, track, TE_SLICE_BEGIN, "MPI wait (wait_sync)", "mpi", NULL, 0, 0, ann, 2);
				} else if (r->ev == LITHE_ST_PHASE_BEGIN) {
					ann[0].name = "hart"; ann[0].val = r->vcore_id;
					pf_event(&o, ts, track, TE_SLICE_BEGIN, st_name_of(r->aux), "phase", NULL, 0, 0, ann, 1);
				} else if (r->ev == LITHE_ST_USER_BEGIN) {
					snprintf(name, sizeof(name), "user %d", r->aux);
					ann[0].name = "hart"; ann[0].val = r->vcore_id;
					pf_event(&o, ts, track, TE_SLICE_BEGIN, name, "user", NULL, 0, 0, ann, 1);
				} else {
					pf_event(&o, ts, track, TE_SLICE_END, NULL, NULL, NULL, 0, 0, NULL, 0);
				}
				break; }
			default:
				break;
			}
			if (r->decision) {
				char fl[200]; fl[0] = 0;
				if (r->decision & LITHE_ST_DEC_STEAL_LOCAL_NONEMPTY) strcat(fl, "steal_with_local_nonempty+");
				if (r->decision & LITHE_ST_DEC_IDLE_WITH_RUNNABLE) strcat(fl, "IDLE_WITH_RUNNABLE+");
				if (r->decision & LITHE_ST_DEC_GRANT_EMPTY_CHILD) strcat(fl, "grant_to_empty_child+");
				if (r->decision & LITHE_ST_DEC_GRANT_PARENT_RUNNABLE) strcat(fl, "grant_while_parent_runnable+");
				if (r->decision & LITHE_ST_DEC_YIELD_WITH_PEERS) strcat(fl, "yield_with_peers+");
				if (r->decision & LITHE_ST_DEC_SELF_RESCHED_HOT) strcat(fl, "self_resched_hot+");
				if (fl[0]) fl[strlen(fl) - 1] = 0;
				ann[0].name = "event"; ann[0].val = r->ev;
				ann[1].name = "ctx"; ann[1].val = r->ctx_id;
				ann[2].name = "aux"; ann[2].val = r->aux;
				HART_EV(TE_INSTANT, fl, "flag", 0, ann, 3);
			}
		}
		if (cur && nrec) {
			/* close a slice still open at dump time (hot-team workers that never
			 * yield); the timestamp must be in the same BOOTTIME domain as the
			 * begin, or the trace processor drops the whole slice (BabelStream
			 * showed 0 % occupancy, 2026-10-17). */
			uint64_t ts = (uint64_t)((int64_t)st_rings[i][(start + nrec - 1) & st_ring_mask].ns + pf_boot_off);
			if (ts <= last_ts) ts = last_ts + 1;
			pf_event(&o, ts, hart, TE_SLICE_END, NULL, NULL, NULL, 0, 0, NULL, 0);
		}
#undef HART_SLICE
#undef HART_EV
	}
	pbo_flush(&o);
	close(fd);
	free(o.b); free(omp_tracks); free(ctx_tracks); free(tcm_tracks); free(wakes);
	if (o.err) {
		fprintf(stderr, "[LITHE_SCHED_TRACE] perfetto write failed: %s\n", strerror(o.err));
		return -1;
	}
	fprintf(stderr, "[LITHE_SCHED_TRACE] dumped perfetto %s records=%llu "
	        "(ui.perfetto.dev / trace_processor)\n", path, (unsigned long long)total);
	return 0;
}

uint32_t lithe_sched_trace_ctx_key(void)
{
	lithe_context_t *c = lithe_context_self();
	if (!c)
		return 0;
	return (uint32_t)c->id;
}

int lithe_sched_trace_dump(void)
{
	int fmt, rc;
	if (!lithe_sched_trace_enabled() || !st_inited)
		return -1;
	st_resolve_path();
	fmt = st_format();
	if (fmt == 0)
		return st_dump_bin(st_path);
	if (fmt == 1)
		return lithe_sched_trace_dump_perfetto(st_path);
	rc = st_dump_bin(st_path);
	{
		char p2[4200];
		snprintf(p2, sizeof(p2), "%s.perfetto-trace", st_path);
		if (lithe_sched_trace_dump_perfetto(p2) != 0)
			rc = -1;
	}
	return rc;
}
