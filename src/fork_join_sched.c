/* Copyright (c) 2014 The Regents of the University of California
 * Kevin Klues <klueska@cs.berkeley.edu>
 * Andrew Waterman <waterman@cs.berkeley.edu>
 * See COPYING for details.
 */

#include <time.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/syscall.h>
#include <sys/mman.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <parlib/waitfreelist.h>
#include <parlib/reactor.h>
/* lithe.c: LITHE_FJS_EXIT_QUIESCE gate (2026-10-15). */
extern int lithe_exit_quiesce_on(void);
#include <parlib/arch.h>
#include "fork_join_sched.h"
#include "lithe.h"
#include "mutex.h"   /* WARM VCORES: park_warm atomically unlocks a lithe_mutex_t */
#include "sched_trace.h"
#include "internal/assert.h"

/* hart_enter idle path: spin briefly before falling through to lithe_hart_yield.
 *
 * Background: when the FJS finishes its work cycle (no children needing harts,
 * empty runqueue, reactor idle) the vcore must yield back to parlib. The
 * yield itself is fairly cheap (vcore_reenter + parent->hart_return + an
 * eventual sys_yield/futex_wait in parlib), but a yield-then-reentry cycle
 * triggered by a context becoming runnable a few hundred ns later wastes
 * thousands of cycles per round. This shows up in lithified flames as a
 * non-trivial slice of vcore_entry / __lithe_sched_reenter / lithe_hart_yield
 * (cf. the post-getenv-cache flame at 4N x 384 r).
 *
 * Mitigation: before yielding, do an adaptive round of cpu_relax-with-checks.
 * If the runqueue, child sched list, or reactor becomes non-empty we jump
 * back into the scheduler decision path; otherwise we yield. Per-vcore
 * adaptive counter (fjs_hart_enter_spin_cur[vcore_id()]) shrinks when the
 * spin failed (we had to yield anyway -- the spin was wasted work) and
 * grows when the spin succeeded (the next event arrived quickly -- worth
 * spinning longer). Bounded to LITHE_FJS_HART_ENTER_SPIN_MIN/_MAX and
 * initialized to LITHE_FJS_HART_ENTER_SPIN_LOOPS. Setting
 * LITHE_FJS_HART_ENTER_SPIN_LOOPS=0 disables the spin entirely (the loop is
 * skipped and we yield immediately, restoring the prior behavior).
 */
#ifndef LITHE_FJS_HART_ENTER_SPIN_LOOPS_DEFAULT
#define LITHE_FJS_HART_ENTER_SPIN_LOOPS_DEFAULT 1024u
#endif
#ifndef LITHE_FJS_HART_ENTER_SPIN_MIN_DEFAULT
#define LITHE_FJS_HART_ENTER_SPIN_MIN_DEFAULT 64u
#endif
#ifndef LITHE_FJS_HART_ENTER_SPIN_MAX_DEFAULT
#define LITHE_FJS_HART_ENTER_SPIN_MAX_DEFAULT 16384u
#endif

static unsigned int fjs_hart_enter_spin_loops = LITHE_FJS_HART_ENTER_SPIN_LOOPS_DEFAULT;
static unsigned int fjs_hart_enter_spin_min = LITHE_FJS_HART_ENTER_SPIN_MIN_DEFAULT;
static unsigned int fjs_hart_enter_spin_max = LITHE_FJS_HART_ENTER_SPIN_MAX_DEFAULT;
static int fjs_hart_enter_spin_loops_init = 0;

/* Per-vcore current spin budget. Padded to a cache line to avoid false
 * sharing across vcores in the adaptive update path. */
#define FJS_SPIN_CL_SIZE 64
struct fjs_spin_slot {
	unsigned int cur;
	char pad[FJS_SPIN_CL_SIZE - sizeof(unsigned int)];
} __attribute__((aligned(FJS_SPIN_CL_SIZE)));

#ifndef LITHE_FJS_HART_ENTER_SPIN_MAX_VCORES
#define LITHE_FJS_HART_ENTER_SPIN_MAX_VCORES 4096
#endif

static struct fjs_spin_slot fjs_hart_enter_spin_cur[LITHE_FJS_HART_ENTER_SPIN_MAX_VCORES];

/* Harts currently blocked in parlib_reactor_drive (infinite or timed). Used so
 * schedule_context only writes wake_efd when a sibling is actually parked —
 * unconditional wake on every enqueue was a syscall tax on the hot path.
 *
 * Lost-wakeup protocol: parker fetch_adds, then rechecks runqueue/pending
 * before drive(); waker enqueues then calls fjs_maybe_reactor_wake(). If the
 * waker races before the add, the recheck sees runnable and skips park; if
 * after drive starts, wake_efd unblocks epoll_wait. */
static int fjs_reactor_parked_harts = 0;

/* Interferer counters (LITHE_FJS_INTERFERE=1): who burns surplus vcores. */
static int fjs_interfere_enabled = -1;
static unsigned long fjs_cnt_idle_yield;
static unsigned long fjs_cnt_surplus_yield;
static unsigned long fjs_cnt_idle_park;
static unsigned long fjs_cnt_hart_request_release;
static unsigned long fjs_cnt_hart_cap_clamp;
static long fjs_sample_owned;
static long fjs_sample_demand;
static long fjs_sample_runnable;

/* B20: when this sched is exiting (teardown_quiesce), leave even if the
 * process-global reactor has pending waiters (they belong to some sched; the
 * parent's harts drive the reactor). LITHE_FJS_QUIESCE_IGNORE_PENDING=0
 * restores the old gate for A/B. Default on. */
static inline int fjs_quiesce_ignore_pending_on(void)
{
  static int cached = -1;
  if (__builtin_expect(cached < 0, 0)) {
    const char *e = getenv("LITHE_FJS_QUIESCE_IGNORE_PENDING");
    cached = (e && e[0] == '0') ? 0 : 1;
  }
  return cached;
}

/* LITHE_FJS_SPIN_QUIESCE (2026-10-24, GAPS B57): the adaptive idle spin in
 * hart_enter (up to LITHE_FJS_HART_ENTER_SPIN_MAX pauses) did not test
 * teardown_quiesce, so an exiting sched's idle harts finished the whole spin
 * before reaching the quiesce gate, and lithe_sched_exit's drain waited for
 * them. =1 leaves the spin as soon as the sched is exiting. */
static inline int fjs_spin_quiesce_on(void)
{
  static int cached = -1;
  if (__builtin_expect(cached < 0, 0)) {
    const char *e = getenv("LITHE_FJS_SPIN_QUIESCE");
    cached = (e && *e) ? (e[0] != '0') : 0;
  }
  return cached;
}

/* LITHE_FJS_IDLE_DIRECT (2026-10-24, GAPS B57; default 0). User: "why should
 * fjs schedulers ever spin?" An FJS with no work spun (adaptive, up to 16384
 * pauses, ~300 us) and then kernel-parked on the reactor (1->64 ms) while
 * holding the hart, instead of returning it to its parent, which is an
 * in-process hand-off (~0.4 us for a whole sched enter+exit). The spin hid the
 * park's wake latency; the spin-vs-sleep choice belongs only at the bottom
 * (parlib's maybe_vcore_yield window, then the kernel). 1 = a CHILD sched
 * (parent is not base) skips the spin and the idle park and yields the hart up
 * at once; 2 = every FJS, the root too (its hart goes back to base/parlib).
 * Reactor waiters (pending > 0, the drive(-1) path) are unchanged.
 * DEFAULT 2 (2026-10-24, with LITHE_VCORE_PARK_SPIN_US=20; =0 restores the
 * spin + park). Login A/B vs the old defaults, 4 harts, n=5 interleaved, every
 * run ec=0 (.build_logs/despin/lithe/final_results.txt): fine libgomp regions
 * 126.6 -> 84.1 us/rep; duty cycle with 50 us gaps region p50 350 -> 244 us,
 * CPU/wall at 500 us gaps 1.73 -> 1.42; nested sched_exit 297 -> 0.4 us;
 * hosted miniFE serial/omp 32^3/48^3, lithe_barrier, nested OpenMP (n=12),
 * climml, hosted stress (ALL CLEAN) at parity. The FJS spin-before-drive
 * (LITHE_FJS_SPIN_BEFORE_DRIVE, MPI processes) is separate and still on: off
 * costs hosted miniFE 48^3 2.5-3.5 % (it hides the reactor driver's eventfd
 * wake). */
static inline int fjs_idle_direct(lithe_fork_join_sched_t *sched)
{
  static int mode = -1;
  if (__builtin_expect(mode < 0, 0)) {
    const char *e = getenv("LITHE_FJS_IDLE_DIRECT");
    mode = (e && *e) ? atoi(e) : 2;   /* default 2 since 2026-10-24, see below */
  }
  if (mode >= 2)
    return 1;
  return mode == 1 && sched->sched.parent && sched->sched.parent->parent;
}

static inline int fjs_interfere_on(void)
{
	if (fjs_interfere_enabled < 0) {
		const char *e = getenv("LITHE_FJS_INTERFERE");
		fjs_interfere_enabled = (e && *e && atoi(e)) ? 1 : 0;
		if (fjs_interfere_enabled)
			fprintf(stderr,
			        "[FJS_INTERFERE] enabled (surplus/idle hart accounting)\n");
	}
	return fjs_interfere_enabled;
}

static void fjs_interfere_atexit(void)
{
	if (fjs_interfere_enabled != 1)
		return;
	fprintf(stderr,
	        "[FJS_INTERFERE] idle_yield=%lu surplus_yield=%lu idle_park=%lu "
	        "hart_request_release=%lu hart_cap_clamp=%lu sample_owned=%ld "
	        "sample_demand=%ld sample_runnable=%ld root_harts_needed=%ld\n",
	        fjs_cnt_idle_yield, fjs_cnt_surplus_yield, fjs_cnt_idle_park,
	        fjs_cnt_hart_request_release, fjs_cnt_hart_cap_clamp,
	        fjs_sample_owned, fjs_sample_demand, fjs_sample_runnable,
	        lithe_root_harts_needed());
	fflush(stderr);
}

/* LITHE_FJS_WAKE_PARKED_ONLY (2026-10-20, DEFAULT ON; =0 restores): poke the reactor
 * only when a hart is parked in it. The `pending > 0` disjunct below makes EVERY context yield and
 * enqueue write the wake eventfd while any reactor waiter exists -- always, in a
 * hosted MPI process (the PMIx progress context sits on its fd), and whenever a
 * timer is pending (perf: a PaRSEC park bounded by a never-firing 10 ms timed futex
 * put 1136 samples of eventfd writes under fjs context_yield, OpenMP co-tenant
 * window 0.146 -> 0.158-0.165). The disjunct closed one race: the infinite driver
 * (drive_parked(-1)) registered and blocked without rechecking the run queue, so a
 * yielder that read parked_harts == 0 just before the increment left its context
 * queued behind a sleeping driver. Under the gate the driver rechecks like the timed
 * parks, and the waker fences between its enqueue and the parked_harts load (Dekker:
 * one side sees the other). Measured (5 interleaved reps, login node): hosted
 * MPI+OpenMP CG 0.0176-0.0189 -> 0.0115-0.0120 s, serial hosted CG 0.0082-0.0085 ->
 * 0.0079-0.0080, hosted allreduce 64 KiB 46.0-46.5 -> 39.1-39.6 us/op, halo 4 KiB
 * 36.5-36.9 -> 27.5-27.9; GPU ranks, hosted ranks K=8, HPX/StarPU/nested OpenMP
 * unchanged; PaRSEC timed park 0.158-0.168 -> 0.147-0.149 (= untimed). Integration,
 * stress, connect/sleep/timed-futex reproducers clean with it on. */
static int fjs_wake_parked_only = -1;
/* For base_hart_request (lithe.c): harts parked in the reactor right now. */
int lithe_fjs_reactor_parked_harts(void)
{
	return __atomic_load_n(&fjs_reactor_parked_harts, __ATOMIC_SEQ_CST);
}
static int fjs_spin_before_drive = -1;
static inline int fjs_spin_before_drive_on(void)
{
	if (fjs_spin_before_drive < 0) {
		const char *e = getenv("LITHE_FJS_SPIN_BEFORE_DRIVE");
		fjs_spin_before_drive = !(e && e[0] == '0');
	}
	return fjs_spin_before_drive;
}
/* LITHE_FJS_WAKE_CHAIN (2026-10-22, GAPS B43): idle harts with no reactor work
 * all park in the same epoll on the one wake eventfd, and N wakes coalesce into
 * one eventfd count that the first woken hart drains, so the others sleep out
 * their 1-64 ms idle-park timeout while contexts sit RUNNABLE. Oversubscribed
 * With the chain, a hart that wakes from the idle park re-pokes when more
 * runnable work and more parked harts remain. NEGATIVE RESULT: written for the
 * B43 tasking stall (8 threads on 4 harts, 1000 regions x 64 tasks, 0.6-90 s),
 * whose stalls shrank with LITHE_FJS_IDLE_PARK_MAX_MS=1 -- but the chain did not
 * change them (8 pairs, both arms stalled). B43 was libomp donating the hart at
 * every ticket-lock release (__kmp_yield_release). Kept, default OFF. */
static int fjs_wake_chain = -1;
static inline int fjs_wake_chain_on(void)
{
	if (fjs_wake_chain < 0) {
		const char *e = getenv("LITHE_FJS_WAKE_CHAIN");
		fjs_wake_chain = (e && *e) ? atoi(e) != 0 : 0;
	}
	return fjs_wake_chain;
}
static int fjs_idle_pending_needs_slot = -1;
static inline int fjs_idle_pending_needs_slot_on(void)
{
	if (fjs_idle_pending_needs_slot < 0) {
		/* default ON since 2026-10-23 (=0 restores): see fjs_pending_is_work */
		const char *e = getenv("LITHE_FJS_IDLE_PENDING_NEEDS_SLOT");
		fjs_idle_pending_needs_slot = (e && *e) ? atoi(e) != 0 : 1;
	}
	return fjs_idle_pending_needs_slot;
}
static inline int fjs_wake_parked_only_on(void)
{
	if (fjs_wake_parked_only < 0) {
		const char *e = getenv("LITHE_FJS_WAKE_PARKED_ONLY");
		fjs_wake_parked_only = !(e && e[0] == '0');
	}
	return fjs_wake_parked_only;
}
static inline void fjs_maybe_reactor_wake(void)
{
	if (fjs_wake_parked_only_on()) {
		__atomic_thread_fence(__ATOMIC_SEQ_CST);
		if (__atomic_load_n(&fjs_reactor_parked_harts, __ATOMIC_SEQ_CST) > 0)
			parlib_reactor_wake();
		return;
	}
	if (__atomic_load_n(&fjs_reactor_parked_harts, __ATOMIC_ACQUIRE) > 0 ||
	    parlib_reactor_pending() > 0)
		parlib_reactor_wake();
}

/* Returns 1 if work appeared before park (caller should restart), 0 after drive. */
static inline int fjs_pending_is_work(void);
static inline int fjs_reactor_drive_parked(lithe_fork_join_sched_t *sched,
                                          int my_vc, int timeout_ms)
{
	__atomic_fetch_add(&fjs_reactor_parked_harts, 1, __ATOMIC_SEQ_CST);
	if (timeout_ms < 0 && fjs_wake_parked_only_on() &&
	    (tqsize_s(sched, my_vc) > 0 ||
	     __atomic_load_n(&sched->runnable_count, __ATOMIC_SEQ_CST) > 0 ||
	     __atomic_load_n(&sched->warm_ready_count, __ATOMIC_SEQ_CST) > 0)) {
		/* the infinite driver's recheck (see fjs_maybe_reactor_wake) */
		__atomic_fetch_sub(&fjs_reactor_parked_harts, 1, __ATOMIC_ACQ_REL);
		return 1;
	}
	/* timed (idle) park: a pending waiter another hart is already driving
	 * is not a reason to skip it -- that bail-out kept the non-driver hart
	 * of a 2-hart DataLoader worker cycling spin -> park -> restart at
	 * 98 % CPU (see fjs_pending_is_work). */
	if (timeout_ms >= 0 &&
	    (fjs_pending_is_work() ||
	     tqsize_s(sched, my_vc) > 0 ||
	     __atomic_load_n(&sched->runnable_count, __ATOMIC_ACQUIRE) > 0 ||
	     __atomic_load_n(&sched->warm_ready_count, __ATOMIC_ACQUIRE) > 0)) {
		__atomic_fetch_sub(&fjs_reactor_parked_harts, 1, __ATOMIC_ACQ_REL);
		return 1;
	}
	(void)parlib_reactor_drive(timeout_ms);
	__atomic_fetch_sub(&fjs_reactor_parked_harts, 1, __ATOMIC_ACQ_REL);
	return 0;
}

static void fjs_hart_enter_spin_init(void)
{
	if (__atomic_load_n(&fjs_hart_enter_spin_loops_init, __ATOMIC_ACQUIRE))
		return;
	const char *env = getenv("LITHE_FJS_HART_ENTER_SPIN_LOOPS");
	if (env != NULL && *env != '\0') {
		char *end = NULL;
		unsigned long v = strtoul(env, &end, 10);
		if (end != env && v <= 1u << 20)
			fjs_hart_enter_spin_loops = (unsigned int)v;
	}
	env = getenv("LITHE_FJS_HART_ENTER_SPIN_MIN");
	if (env != NULL && *env != '\0') {
		char *end = NULL;
		unsigned long v = strtoul(env, &end, 10);
		if (end != env && v <= 1u << 20)
			fjs_hart_enter_spin_min = (unsigned int)v;
	}
	env = getenv("LITHE_FJS_HART_ENTER_SPIN_MAX");
	if (env != NULL && *env != '\0') {
		char *end = NULL;
		unsigned long v = strtoul(env, &end, 10);
		if (end != env && v <= 1u << 20)
			fjs_hart_enter_spin_max = (unsigned int)v;
	}
	if (fjs_hart_enter_spin_max < fjs_hart_enter_spin_min)
		fjs_hart_enter_spin_max = fjs_hart_enter_spin_min;
	if (fjs_hart_enter_spin_loops > fjs_hart_enter_spin_max)
		fjs_hart_enter_spin_loops = fjs_hart_enter_spin_max;
	for (size_t i = 0; i < LITHE_FJS_HART_ENTER_SPIN_MAX_VCORES; i++)
		fjs_hart_enter_spin_cur[i].cur = fjs_hart_enter_spin_loops;
	__atomic_store_n(&fjs_hart_enter_spin_loops_init, 1, __ATOMIC_RELEASE);
}

static inline unsigned int fjs_hart_enter_spin_budget(int vc)
{
	if (vc < 0 || vc >= LITHE_FJS_HART_ENTER_SPIN_MAX_VCORES)
		return fjs_hart_enter_spin_loops;
	return fjs_hart_enter_spin_cur[vc].cur;
}

/* AIMD-style update: success doubles (capped at max), failure halves
 * (floor at min). One write per hart_enter idle path -- no atomics: the
 * value is a per-vcore hint, not a correctness signal, and stale reads
 * just produce a slightly stale spin budget for one round. */
static inline void fjs_hart_enter_spin_update(int vc, int succeeded)
{
	if (vc < 0 || vc >= LITHE_FJS_HART_ENTER_SPIN_MAX_VCORES)
		return;
	unsigned int v = fjs_hart_enter_spin_cur[vc].cur;
	if (succeeded) {
		unsigned int nv = v ? (v * 2u) : fjs_hart_enter_spin_min;
		if (nv > fjs_hart_enter_spin_max) nv = fjs_hart_enter_spin_max;
		fjs_hart_enter_spin_cur[vc].cur = nv;
	} else {
		unsigned int nv = v / 2u;
		if (nv < fjs_hart_enter_spin_min) nv = fjs_hart_enter_spin_min;
		fjs_hart_enter_spin_cur[vc].cur = nv;
	}
}

/* The legacy per-vcore nanosleep idle backoff is gone. Reasoning (per
 * user directive: "the nanosleeps are wrong, a non-schedulable context
 * should just wait. there should be a way for the scheduler to poll
 * for system calls that have finished and serve the continuation that
 * way."):
 *
 *   - nanosleep is the wrong primitive. Either we wait too long
 *     (stalls MPI progress, ~37x slowdown observed at 200us) or we
 *     wake too often (scheduler thrash, ~120x slowdown observed at
 *     10us).
 *   - The right primitive is epoll_wait on the parlib reactor. The
 *     kernel parks the vcore pthread until EITHER a parked uthread's
 *     fd fires OR a sibling vcore writes wake_efd (new context queued
 *     or hart request from a child sched). Wakeup latency is one
 *     syscall; idle cost is zero CPU.
 *   - hart_enter handles the wait directly via parlib_reactor_drive.
 *     There's no fallback path that uses nanosleep.
 *
 * lithe_fork_join_set_idle_nanosleep_usec is retained as a no-op for
 * ABI compatibility with callers in OMPI's MPI_Finalize teardown that
 * historically twiddled this knob. The argument is silently ignored. */
void lithe_fork_join_set_idle_nanosleep_usec(long us)
{
    (void)us;
}

/* LITHE_FJS_REACTOR_POLL_US (2026-10-17): minimum interval between two
 * zero-timeout reactor polls on one hart. pending is never 0 in a lithified
 * MPI process (the PMIx progress context is parked on its fd for the life of
 * the process), so the gate below polled on EVERY hart re-enter: epoll_wait(0)
 * + two clock_gettime in parlib_reactor_drive. perf (audit 2026-10-17):
 * 8 % of GROMACS NP=1 OMP=4 samples, 81 % of a hosted RPH=8 run's. The reactor
 * serves PMIx/libevent sockets and timers, which tolerate tens of us; MPI
 * progress does not go through it. 0 = poll every time (old). Default 50 after
 * the A/B (3 reps): hosted RPH=4 x OMP=2 CG 0.0236 -> 0.0187 s, RPH=8 0.0174 ->
 * 0.0164, serial 0.0087 -> 0.0084, stress 46.6 -> 45.9 us/op, LULESH +2 %,
 * GROMACS / LAMMPS within noise. */
static long fjs_reactor_poll_us = 50;
static uint64_t fjs_tsc_per_us = 0;
static __thread uint64_t fjs_last_poll_tsc;
static inline uint64_t fjs_rdtsc(void)
{
	unsigned int lo, hi;
	__asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
	return ((uint64_t)hi << 32) | lo;
}
static void fjs_reactor_poll_init(void)
{
	const char *e = getenv("LITHE_FJS_REACTOR_POLL_US");
	if (e && *e) {
		char *end = NULL;
		unsigned long v = strtoul(e, &end, 10);
		if (end != e && v <= 100000ul)
			fjs_reactor_poll_us = (long)v;
	}
	if (fjs_reactor_poll_us > 0) {
		/* calibrate the TSC once against CLOCK_MONOTONIC. 100 us (was 2 ms, a
		 * busy wait every Lithe process paid before its first context ran:
		 * 2.1 ms of a ~4 ms bootstrap) is well under 1 % error for the poll
		 * interval it scales. */
		struct timespec a, b;
		uint64_t t0 = fjs_rdtsc(), t1;
		long ns;
		clock_gettime(CLOCK_MONOTONIC, &a);
		do {
			clock_gettime(CLOCK_MONOTONIC, &b);
			ns = (b.tv_sec - a.tv_sec) * 1000000000L + (b.tv_nsec - a.tv_nsec);
		} while (ns < 100000L);
		t1 = fjs_rdtsc();
		fjs_tsc_per_us = (t1 - t0) * 1000 / (uint64_t)ns;
		if (fjs_tsc_per_us == 0)
			fjs_tsc_per_us = 1;
	}
}
static inline void fjs_reactor_poll(void)
{
	/* parlib_reactor_drive(0) is epoll_wait(..., 0). When no uthreads are
	 * parked on fd readiness (pending==0), that syscall is pure overhead on
	 * every hart_enter restart — dominant in MPI_Init/Finalize envelope
	 * profiles at full-node rank count. hart_request still pokes wake_efd
	 * for vcores blocked in drive(-1); runqueue work is found via dequeue
	 * and the adaptive spin without needing a zero-timeout poll here. */
	if (!parlib_reactor_initialized() || parlib_reactor_pending() <= 0)
		return;
	if (fjs_reactor_poll_us > 0) {
		uint64_t t = fjs_rdtsc();
		if (t - fjs_last_poll_tsc < (uint64_t)fjs_reactor_poll_us * fjs_tsc_per_us)
			return;
		fjs_last_poll_tsc = t;
	}
	(void)parlib_reactor_drive(0);
}

static struct wfl sched_zombie_list = WFL_INITIALIZER(sched_zombie_list);

/* ---- context free list, BUCKETED BY STACK SIZE CLASS --------------------
 *
 * CORRECTNESS FIX (2026-10-13). This used to be a single process-global list
 * carrying the comment "wfl currently assumes stacksize the same for all
 * contexts". That assumption is FALSE in this stack:
 *
 *   OPAL progress contexts   256 KB  (opal/mca/threads/lithe/threads_lithe_module.c)
 *   PMIx progress contexts     1 MB  (openpmix/src/threads/thread.c)
 *   miniFE rank contexts      16 MB  (4 * FJS_STACK_SIZE)
 *   OFI helpers              varies  (ofi_thread_create)
 *
 * __ctx_alloc() popped the list with no size check, so a recycled 256 KB
 * progress context could be handed to a rank asking for 16 MB -- a 64x
 * undersized stack, i.e. silent memory corruption with an arbitrary blast
 * radius. Any context destroy (lithe_fork_join_context_destroy -> __ctx_free)
 * feeds the same list, so mixing is reachable as soon as one progress helper
 * exits before the ranks are created.
 *
 * Fix: bucket by ceil(log2(pages)) and only reuse within a class, plus a
 * belt-and-braces size check on reuse (the per-context offset randomisation
 * means usable size varies slightly within a class).
 *
 * LITHE_FJS_CTX_POOL=0 reverts to a single unbucketed list for A/B only --
 * it reinstates the bug, so use it to demonstrate, never to run.
 * -------------------------------------------------------------------------- */
#define FJS_CTX_SIZE_CLASSES 28
static struct wfl context_zombie_class[FJS_CTX_SIZE_CLASSES];
static struct wfl context_zombie_list = WFL_INITIALIZER(context_zombie_list);

static void fjs_ctx_pool_ctor(void) __attribute__((constructor(102)));
static void fjs_ctx_pool_ctor(void)
{
	for (int i = 0; i < FJS_CTX_SIZE_CLASSES; i++)
		wfl_init(&context_zombie_class[i]);
}

static int fjs_ctx_pool_bucketed(void)
{
	static int cached = -1;
	if (__builtin_expect(cached < 0, 0)) {
		const char *e = getenv("LITHE_FJS_CTX_POOL");
		int v = 1;
		if (e && e[0] == '0' && e[1] == '\0')
			v = 0;
		__atomic_store_n(&cached, v, __ATOMIC_RELAXED);
	}
	return __atomic_load_n(&cached, __ATOMIC_RELAXED) != 0;
}

static int fjs_ctx_size_class(size_t stacksize)
{
	size_t pages = (stacksize + PGSIZE - 1) / PGSIZE;
	int c = 0;
	while (((size_t)1 << c) < pages && c < FJS_CTX_SIZE_CLASSES - 1)
		c++;
	return c;
}

/* Class a FREED context is pooled under (2026-10-18). __ctx_alloc looks up
 * ceil(log2(requested pages)); a freed context's usable size is the request
 * plus page rounding minus the cache-line offset, i.e. one page MORE than a
 * power-of-two request, so the ceiling put it one class too high and no
 * power-of-two request (64 KB .. 16 MB, i.e. all of them) ever hit the pool:
 * every create mmap'd, every free munmap'd once the class held 1000
 * (bench/alt/ulth_bench.c create: 20% mmap + 20% munmap). Pool by the floor
 * instead: a context in class c has >= 2^c pages, which satisfies any request
 * that maps to class c; the size check in __ctx_alloc still guards reuse.
 * LITHE_FJS_CTX_POOL_FLOOR=0 restores the ceiling, for A/B only. */
static int fjs_ctx_free_class(size_t usable)
{
	static int floor_on = -1;
	if (__builtin_expect(floor_on < 0, 0)) {
		const char *e = getenv("LITHE_FJS_CTX_POOL_FLOOR");
		floor_on = !(e && e[0] == '0' && e[1] == '\0');
	}
	if (!floor_on)
		return fjs_ctx_size_class(usable);
	size_t pages = usable / PGSIZE;
	int c = 0;
	while (((size_t)2 << c) <= pages && c < FJS_CTX_SIZE_CLASSES - 1)
		c++;
	return c;
}

/* Executable context stacks are not needed by anything here; PROT_EXEC also
 * makes the mapping ineligible for THP on some kernels. LITHE_FJS_STACK_EXEC=1
 * restores the old PROT_READ|PROT_WRITE|PROT_EXEC if something turns out to
 * need it. */
static int fjs_stack_exec(void)
{
	static int cached = -1;
	if (__builtin_expect(cached < 0, 0)) {
		const char *e = getenv("LITHE_FJS_STACK_EXEC");
		__atomic_store_n(&cached, (e && e[0] == '1') ? 1 : 0, __ATOMIC_RELAXED);
	}
	return __atomic_load_n(&cached, __ATOMIC_RELAXED) != 0;
}

/* madvise(MADV_HUGEPAGE) on stacks >= 2 MB. Advisory: a no-op when THP is
 * "never", a 512x reduction in fault COUNT when it is "madvise" or "always".
 * Context stacks are the largest single mapping this runtime makes (16 MB per
 * miniFE rank), so they are where fault count concentrates. 0 to disable. */
static int fjs_stack_hugepage(void)
{
	static int cached = -1;
	if (__builtin_expect(cached < 0, 0)) {
		const char *e = getenv("LITHE_FJS_STACK_HUGE");
		int v = 1;
		if (e && e[0] == '0' && e[1] == '\0')
			v = 0;
		__atomic_store_n(&cached, v, __ATOMIC_RELAXED);
	}
	return __atomic_load_n(&cached, __ATOMIC_RELAXED) != 0;
}

/* Env-gated concurrency instrumentation (LITHE_FJS_STATS=1). Tracks the number
 * of contexts currently RUNNING (dispatched via hart_enter and not yet
 * blocked/yielded/exited) and prints whenever a NEW peak is reached. This
 * directly answers "do OMP worker contexts run CONCURRENTLY on distinct
 * vcores, or serialize on one?": a peak of ~OMP means concurrent; a peak of 1
 * means serialized. Printed LIVE (not at exit) because the lithified process
 * tears down via a path that bypasses .so destructors. Zero cost when disabled
 * (one cached int load on the run path). Plain atomics in the lithe substrate
 * -- no Linux sync primitives. The master context runs inline (not via
 * hart_enter) so the peak may undercount by ~1; immaterial for 1-vs-OMP. */
static int fjs_stats_enabled = -1;
static long fjs_running;   /* contexts currently in RUNNING state */
static long fjs_peak;      /* max observed concurrent running contexts */
static inline int fjs_stats_on(void)
{
	if (fjs_stats_enabled < 0) {
		const char *e = getenv("LITHE_FJS_STATS");
		fjs_stats_enabled = (e && *e && atoi(e)) ? 1 : 0;
		if (fjs_stats_enabled)
			fprintf(stderr, "[FJS_STATS] enabled (live peak-concurrency probe)\n");
	}
	return fjs_stats_enabled;
}
static inline void fjs_stats_run_enter(void)
{
	if (!fjs_stats_on())
		return;
	long n = __sync_add_and_fetch(&fjs_running, 1);
	long p = fjs_peak;
	if (n > p && __sync_bool_compare_and_swap(&fjs_peak, p, n))
		fprintf(stderr, "[FJS_STATS] peak concurrent running contexts = %ld\n", n);
}
static inline void fjs_stats_run_leave(void)
{
	if (fjs_stats_enabled != 1)
		return;
	if (fjs_running > 0)
		__sync_fetch_and_sub(&fjs_running, 1);
}

/* ---------------------------------------------------------------------------
 * Scheduler tradespace policy knobs (env parsed once at first use).
 * LITHE_FJS_STEAL={default,none,local}
 * LITHE_PROGRESS_SCHED={root,child}
 * LITHE_FJS_HART_ORDER={child_first,progress_first,self_first}
 * LITHE_PROGRESS_SPIN_MAX=N  (bounded spin before reactor block; see header)
 * ------------------------------------------------------------------------- */
static lithe_fjs_steal_mode_t fjs_steal_mode = LITHE_FJS_STEAL_DEFAULT;
static int fjs_steal_topo; /* LITHE_FJS_STEAL_TOPO, A6 */
/* Per-thief victim order: vcores sorted by (same L3, same NUMA, other), built
 * lazily once per vcore from parlib's topology accessors. */
#define FJS_TOPO_MAX_VC 256
static int16_t fjs_topo_order[FJS_TOPO_MAX_VC][FJS_TOPO_MAX_VC];
static int fjs_topo_n[FJS_TOPO_MAX_VC];
static int fjs_topo_ready[FJS_TOPO_MAX_VC];
/* A11 (docs/PLAN-affinity.md, GAPS C15): L3 confinement. LITHE_FJS_L3_CONFINE=1: a hart outside the
 * L3 group of vcore 0 (the "home" group, where the rank contexts' data lives) runs no context while
 * any home hart of this sched is online and idle; it pokes the reactor so the idle home hart wakes.
 * Hosted RPH=8: the same 8 harts give CG 4.9 ms in one group and 9.8 ms split across two. */
static int fjs_l3_confine = -1;
static unsigned char fjs_vc_busy[FJS_TOPO_MAX_VC];
static signed char fjs_vc_home[FJS_TOPO_MAX_VC]; /* -1 unknown, 0 no, 1 yes */
static inline void fjs_vc_set_busy(int on)
{
	int v = vcore_id();
	if (v >= 0 && v < FJS_TOPO_MAX_VC)
		__atomic_store_n(&fjs_vc_busy[v], (unsigned char)on, __ATOMIC_RELEASE);
}
static void fjs_l3_confine_init(void)
{
	/* Default: on for hosted multi-rank processes (LITHE_CONTEXT_RANKS_PER_HOST > 1), whose rank
	 * contexts migrate between harts: RPH=8 CG 8.2-8.8 -> 5.9-7.0 ms at 12 harts, 8.5-8.7 -> 5.4-6.4 at
	 * 16, neutral within one group. Off for flat OpenMP teams, which want every hart and lost up to 6 %
	 * (XSBench OMP=16 spanning two groups). LITHE_FJS_L3_CONFINE=0/1 overrides. */
	const char *e = getenv("LITHE_FJS_L3_CONFINE");
	if (e && *e) {
		fjs_l3_confine = (atoi(e) != 0);
	} else {
		const char *r = getenv("LITHE_CONTEXT_RANKS_PER_HOST");
		fjs_l3_confine = (r && atoi(r) > 1) ? 1 : 0;
	}
	if (!fjs_l3_confine) return;
	int mv = (int)max_vcores(), g0 = parlib_vcore_l3_group(0);
	for (int v = 0; v < mv && v < FJS_TOPO_MAX_VC; v++) {
		int g = parlib_vcore_l3_group(v);
		/* unknown placement (unpinned): treat as home, i.e. no confinement */
		fjs_vc_home[v] = (g0 < 0 || g < 0 || g == g0) ? 1 : 0;
	}
}
static void fjs_topo_build(int vc, int mv)
{
	int l3 = parlib_vcore_l3_group(vc), nd = parlib_vcore_numa_node(vc);
	int n = 0;
	if (mv > FJS_TOPO_MAX_VC)
		mv = FJS_TOPO_MAX_VC;
	for (int pass = 0; pass < 3; pass++) {
		for (int i = 0; i < mv; i++) {
			if (i == vc)
				continue;
			int il3 = parlib_vcore_l3_group(i), ind = parlib_vcore_numa_node(i);
			int cls = (il3 >= 0 && il3 == l3) ? 0 : ((ind >= 0 && ind == nd) ? 1 : 2);
			if (cls == pass)
				fjs_topo_order[vc][n++] = (int16_t)i;
		}
	}
	fjs_topo_n[vc] = n;
	__atomic_store_n(&fjs_topo_ready[vc], 1, __ATOMIC_RELEASE);
}
static lithe_fjs_hart_order_t fjs_hart_order = LITHE_FJS_HART_ORDER_CHILD_FIRST;
static int fjs_progress_sched_child = 0;
static int fjs_progress_child_drive_v = -1;
static int fjs_progress_child_drive(void)
{
	if (__builtin_expect(fjs_progress_child_drive_v < 0, 0)) {
		const char *e = getenv("LITHE_PROGRESS_CHILD_DRIVE");
		fjs_progress_child_drive_v = (e && atoi(e) > 0) ? 1 : 0;
	}
	return fjs_progress_child_drive_v;
}
static int fjs_tradespace_init = 0;
/* Soft cap on FJS online harts (0 = unset). From LITHE_FJS_MAX_ONLINE_HARTS
 * or LITHE_CONTEXT_RANKS_PER_HOST (+ helper budget). Clamps *helper* hart
 * demand so progress contexts do not claim the full VCORE pool (diagnostic:
 * LITHE_FJS_STATS peak RUNNING inflated from helper misuse, e.g. peak=4 on
 * P1K2 when VCORE≥4 with no cap). It is NOT the product model of "what ranks
 * are" — ranks are Lithe contexts; Flux OS processes are quiesced onto
 * vcores. Positive hart requests are clamped so owned never exceeds the cap;
 * wait-spinners yield when owned >= cap with RUNNABLE peers.
 *
 * Default when only RPH is set: soft_cap = RPH + LITHE_FJS_HELPER_HARTS
 * (default helper budget 0). After MTL unlock-around-park, soft_cap==RPH
 * multiplexes correctly without the old lock-hold serialization; raising
 * helpers (RPH+1) helps some single-OS shapes but regresses multi-OS
 * park+pump (P2K2 ~2ms). Opt in via LITHE_FJS_HELPER_HARTS=1.
 * LITHE_FJS_MAX_ONLINE_HARTS still wins when set. */
static int fjs_soft_hart_cap = 0;
/* Idle harts allowed in the infinite reactor drive (LITHE_FJS_REACTOR_DRIVERS; 0 = all = the
 * pre-2026-10-17 behaviour, see hart_enter). Default 1 after the A/B: XSBench NP=2 OMP=4
 * 13.2-13.5M -> 25.3-26.1M lookups/s (vanilla 26.2-27.1M), LULESH OMP=4 lost its half-speed
 * mode (1454 vs 2710-2787 z/s), GROMACS and LAMMPS unchanged. */
static long fjs_reactor_drivers_max = 1;
static long fjs_reactor_drivers = 0;
/* Is a pending reactor waiter work for THIS idle hart? Only if it could take a
 * driver slot. Default ON since 2026-10-23: lithified CPython at VCORE_LIMIT=4
 * burned 2.99 s CPU per 1 s time.sleep (three surplus harts spinning on the
 * sleep's own timer; test_time test_process_time), 0.000 s with the gate.
 * Under LITHE_FJS_IDLE_PENDING_NEEDS_SLOT=1 (=0 restores) a hart that cannot
 * drive treats pending>0 as no work at every idle exit (spin-before-drive
 * entry, adaptive spin, park recheck, leave guard), so it parks and then
 * yields its hart; the slot holder serves the reactor and wakes contexts
 * through vcore_request. Before (2026-10-22), every one of those exits
 * restarted on pending>0, so in a process with any cooperative fd waiter the
 * surplus harts never left: fork()ed DataLoader workers with 2 harts spun the
 * second one at 97 % in the spin-before-drive loop and the trainer sharing
 * their CPUs fell from 24 to 1.5 images/s. */
static inline int fjs_pending_is_work(void)
{
	if (parlib_reactor_pending() <= 0)
		return 0;
	if (fjs_idle_pending_needs_slot_on() && fjs_reactor_drivers_max > 0 &&
	    __atomic_load_n(&fjs_reactor_drivers, __ATOMIC_ACQUIRE) >= fjs_reactor_drivers_max)
		return 0;
	return 1;
}
/* DRIVER HANDOFF (LITHE_FJS_DRIVER_HANDOFF, default on, =0 off; 2026-10-23). The driver
 * slot holder leaves the drive to run work, and the harts that lost the slot have already
 * parked and yielded to parlib (fjs_pending_is_work), so an armed timer has no hart left to
 * fire it unless a busy hart re-enters. A context parked by the glibc futex hook waits on
 * such a timer (its slice; a cross-process FUTEX_WAKE never reaches it otherwise), and
 * oneDNN's backward convolution spins three OpenMP threads in a JIT barrier that never
 * yields while the fourth sits parked: lithified PyTorch dataloader_train hung 6/13 with
 * the futex hook, 0/8 without (.build_logs/glibc_lithe/dl_spawn). So when the driver leaves
 * with runnable work and timers still armed, it asks for one hart to take the slot (granted
 * only below the hart limit; the idle-yield surplus path returns it). fd waiters without a
 * deadline do not trigger it: in a lithified MPI process they are always pending. */
static int fjs_driver_handoff_mode = -1;
static void fjs_driver_handoff(lithe_fork_join_sched_t *sched)
{
	if (fjs_driver_handoff_mode < 0) {
		const char *e = getenv("LITHE_FJS_DRIVER_HANDOFF");
		fjs_driver_handoff_mode = !(e && e[0] == '0');
	}
	if (!fjs_driver_handoff_mode || !parlib_reactor_timers_pending() ||
	    __atomic_load_n(&fjs_reactor_drivers, __ATOMIC_ACQUIRE) > 0)
		return;
	if (tqsize_s(sched, vcore_id()) > 0 ||
	    __atomic_load_n(&sched->runnable_count, __ATOMIC_ACQUIRE) > 0 ||
	    __atomic_load_n(&sched->warm_ready_count, __ATOMIC_ACQUIRE) > 0)
		lithe_hart_request(1);
}
/* Park-budget knobs (2026-10-17, audit section 3.5 / 8): how long an idle hart stays
 * resident before it is returned to parlib, and how much surplus is tolerated.
 *   LITHE_FJS_IDLE_PARK_ROUNDS  (default 24): timed-park rounds (1,1,4,4,...,MAX ms) before idle_yield
 *   LITHE_FJS_IDLE_PARK_MAX_MS  (default 64): ceiling of the park escalation
 *   LITHE_FJS_SURPLUS_SLACK     (default 1): a hart is surplus when owned > runnable + warm + 1 + slack
 * A resident hart dispatches a wake in ~1 us; a returned one needs vcore_request + futex, 88 us p50 /
 * 270 us p90 at RPH=8 (docs/AUDIT-perfetto-performance-20261017.md). */
static unsigned fjs_idle_park_rounds = 24u;
static int fjs_idle_park_max_ms = 64;
static long fjs_surplus_slack = 1;
/* Extra harts above RPH reserved for progress/helpers (default 0). */
static int fjs_helper_harts = 0;
/* Default: spin before CQ park. 0 meant every empty progress tick paid
 * parlib_reactor_wait/epoll — ~0.5–0.8ms Barrier+Allreduce for hosted P1K2
 * vs ~8us vanilla. MTL CQ spin now yields when runnable_count>0 so this is
 * safe under oversubscription (hosted ranks / OMP>harts). Override with
 * LITHE_PROGRESS_SPIN_MAX=0 to restore immediate park. */
#ifndef LITHE_PROGRESS_SPIN_MAX_DEFAULT
#define LITHE_PROGRESS_SPIN_MAX_DEFAULT 4096u
#endif
static unsigned int fjs_progress_spin_max = LITHE_PROGRESS_SPIN_MAX_DEFAULT;

static lithe_fork_join_sched_t *lithe_progress_child_sched;
static int lithe_progress_child_registered;

static void fjs_tradespace_init_once(void)
{
	if (__atomic_load_n(&fjs_tradespace_init, __ATOMIC_ACQUIRE))
		return;

	const char *e = getenv("LITHE_FJS_STEAL");
	if (!e || !*e || strcmp(e, "default") == 0)
		fjs_steal_mode = LITHE_FJS_STEAL_DEFAULT;
	else if (strcmp(e, "none") == 0)
		fjs_steal_mode = LITHE_FJS_STEAL_NONE;
	else if (strcmp(e, "local") == 0)
		fjs_steal_mode = LITHE_FJS_STEAL_LOCAL;
	else
		fjs_steal_mode = LITHE_FJS_STEAL_DEFAULT;

	e = getenv("LITHE_PROGRESS_SCHED");
	fjs_progress_sched_child = (e && strcmp(e, "child") == 0) ? 1 : 0;

	/* A6 (docs/PLAN-affinity.md): LITHE_FJS_STEAL_TOPO=1 orders steal victims
	 * by distance from the thief's CPU -- same L3 group, then same NUMA
	 * node, then the rest -- instead of two random picks + a round-robin
	 * walk from vcoreid+1. A stolen context drags its cache footprint and,
	 * on first touch, its pages across the boundary it crosses. Measured
	 * 2026-10-16 (login node, hosted_allreduce_stress, 16 contexts on 12
	 * harts = CPUs 0-11 across the L3 boundary at 8, 3 reps): allreduce 64 KiB
	 * random 306/300/291 us/op -> topo 267/290/268; halo 4 KiB noise either
	 * way; control (8 ctx on 6 harts inside one L3) 91 both. DEFAULT ON;
	 * LITHE_FJS_STEAL_TOPO=0 restores the random pair + round-robin walk. */
	e = getenv("LITHE_FJS_STEAL_TOPO");
	fjs_steal_topo = (e && *e) ? (atoi(e) != 0) : 1;

	e = getenv("LITHE_FJS_HART_ORDER");
	if (!e || !*e || strcmp(e, "child_first") == 0)
		fjs_hart_order = LITHE_FJS_HART_ORDER_CHILD_FIRST;
	else if (strcmp(e, "progress_first") == 0)
		fjs_hart_order = LITHE_FJS_HART_ORDER_PROGRESS_FIRST;
	else if (strcmp(e, "self_first") == 0)
		fjs_hart_order = LITHE_FJS_HART_ORDER_SELF_FIRST;
	else
		fjs_hart_order = LITHE_FJS_HART_ORDER_CHILD_FIRST;

	e = getenv("LITHE_PROGRESS_SPIN_MAX");
	if (e && *e) {
		char *end = NULL;
		unsigned long v = strtoul(e, &end, 10);
		if (end != e && v <= (1u << 20))
			fjs_progress_spin_max = (unsigned int)v;
	}

	fjs_reactor_poll_init();
	e = getenv("LITHE_FJS_IDLE_PARK_ROUNDS");
	if (e && *e) { char *end = NULL; unsigned long v = strtoul(e, &end, 10); if (end != e && v <= 100000ul) fjs_idle_park_rounds = (unsigned)v; }
	e = getenv("LITHE_FJS_IDLE_PARK_MAX_MS");
	if (e && *e) { char *end = NULL; unsigned long v = strtoul(e, &end, 10); if (end != e && v >= 1 && v <= 10000ul) fjs_idle_park_max_ms = (int)v; }
	e = getenv("LITHE_FJS_SURPLUS_SLACK");
	if (e && *e) { char *end = NULL; unsigned long v = strtoul(e, &end, 10); if (end != e && v <= 1024ul) fjs_surplus_slack = (long)v; }
	e = getenv("LITHE_FJS_REACTOR_DRIVERS");
	if (e && *e) {
		char *end = NULL;
		unsigned long v = strtoul(e, &end, 10);
		if (end != e && v <= 1024ul)
			fjs_reactor_drivers_max = (long)v;
	}
	e = getenv("LITHE_FJS_HELPER_HARTS");
	if (e && *e) {
		char *end = NULL;
		unsigned long v = strtoul(e, &end, 10);
		/* 0 = strict RPH (old multiplex-only); clamp to a small budget so
		 * soft_cap never tracks 2×RPH (that reintroduces surplus tax). */
		if (end != e && v <= 8ul)
			fjs_helper_harts = (int)v;
	}

	e = getenv("LITHE_FJS_MAX_ONLINE_HARTS");
	if (e && *e) {
		char *end = NULL;
		unsigned long v = strtoul(e, &end, 10);
		if (end != e && v >= 1 && v <= (unsigned long)parlib_vcore_limit())
			fjs_soft_hart_cap = (int)v;
	} else {
		e = getenv("LITHE_CONTEXT_RANKS_PER_HOST");
		if (e && *e) {
			char *end = NULL;
			unsigned long v = strtoul(e, &end, 10);
			if (end != e && v >= 1 && v <= (unsigned long)parlib_vcore_limit()) {
				unsigned long cap = v + (unsigned long)fjs_helper_harts;
				/* Hosted MPI+OpenMP (2026-10-18): with OMP_NUM_THREADS=T each rank's
				 * team wants T harts, but the RPH cap held ALL ranks' teams to RPH
				 * harts (2 ranks x 8 threads on 8 harts ran on 2: 41 ms/iter vs 10.4
				 * with the cap at 8; bench/alt/ranks_omp.c). Cap at RPH*T instead,
				 * still clamped to the vcore pool. LITHE_FJS_CAP_OMP=0 restores RPH. */
				const char *ce = getenv("LITHE_FJS_CAP_OMP");
				const char *te = getenv("OMP_NUM_THREADS");
				if (!(ce && ce[0] == '0' && ce[1] == '\0') && te && *te) {
					unsigned long t = strtoul(te, NULL, 10);
					if (t > 1)
						cap = v * t + (unsigned long)fjs_helper_harts;
				}
				if (cap > (unsigned long)parlib_vcore_limit())
					cap = (unsigned long)parlib_vcore_limit();
				/* soft_cap = RPH + helper budget (MAX_ONLINE_HARTS wins above). */
				fjs_soft_hart_cap = (int)cap;
			}
		}
	}

	__atomic_store_n(&fjs_tradespace_init, 1, __ATOMIC_RELEASE);
}

lithe_fjs_steal_mode_t lithe_fork_join_get_steal_mode(void)
{
	fjs_tradespace_init_once();
	return fjs_steal_mode;
}

lithe_fjs_hart_order_t lithe_fork_join_get_hart_order(void)
{
	fjs_tradespace_init_once();
	return fjs_hart_order;
}

unsigned int lithe_progress_spin_max(void)
{
	fjs_tradespace_init_once();
	return fjs_progress_spin_max;
}

int lithe_fork_join_sched_is_progress_child(lithe_sched_t *sched)
{
	fjs_tradespace_init_once();
	return lithe_progress_child_sched != NULL &&
	       sched == (lithe_sched_t *)lithe_progress_child_sched;
}

lithe_fork_join_sched_t *lithe_fork_join_sched_progress_sched(void)
{
	fjs_tradespace_init_once();
	lithe_fork_join_sched_t *root = lithe_ensure_root_fork_join_sched();
	if (root == NULL)
		return NULL;
	if (!fjs_progress_sched_child)
		return root;

	if (lithe_progress_child_sched == NULL) {
		lithe_fork_join_sched_t *s = lithe_fork_join_sched_create();
		if (s == NULL)
			return root;
		/* Parent set before publication: a racing caller may create a context on it
		 * before registration below finishes (GAPS E9). */
		s->sched.parent = (lithe_sched_t *)root;
		s->sched.harts = PARLIB_ATOMIC_INITIALIZER(0);
		if (!__sync_bool_compare_and_swap(&lithe_progress_child_sched, NULL, s))
			lithe_fork_join_sched_destroy(s);
	}

	/* GAPS E9 (2026-10-25): this child is registered in the root's child list but never
	 * entered by lithe_sched_enter(), so nothing set its parent (the first context created on
	 * it SIGSEGV'd in lithe_hart_request: parent NULL, 6/6) and child_enter's 1/1 ledger --
	 * which assumes the entering hart is the child's first hart -- claimed a hart it never
	 * held, and the old "needed >= 1" pin made that claim permanent. Wire it the way
	 * lithe_sched_enter would, but with an empty 0/0 ledger: demand now comes only from
	 * schedule_context -> lithe_hart_request_for(child) and is released by the child's
	 * idle-yield path like any other child's. */
	if (!lithe_progress_child_registered && lithe_progress_child_sched != NULL &&
	    __sync_bool_compare_and_swap(&lithe_progress_child_registered, 0, 1)) {
		lithe_sched_t *c = (lithe_sched_t *)lithe_progress_child_sched;
		lithe_fork_join_sched_child_enter((lithe_sched_t *)root, c);
		uint16_t *harts_granted = (uint16_t *)&c->parent_data;
		uint16_t *harts_needed = (uint16_t *)&c->parent_data + 1;
		*harts_granted = 0;
		*harts_needed = 0;
	}

	return lithe_progress_child_sched ? lithe_progress_child_sched : root;
}

lithe_fork_join_context_t *
lithe_fork_join_context_create_progress(size_t stack_size,
                                        void (*start_routine)(void*),
                                        void *arg)
{
	lithe_fork_join_sched_t *sched = lithe_fork_join_sched_progress_sched();
	if (sched == NULL)
		return NULL;
	return lithe_fork_join_context_create(sched, stack_size, start_routine, arg);
}

const lithe_sched_funcs_t lithe_fork_join_sched_funcs = {
  .hart_request    = lithe_fork_join_sched_hart_request,
  .hart_enter      = lithe_fork_join_sched_hart_enter,
  .hart_return     = lithe_fork_join_sched_hart_return,
  .sched_enter     = lithe_fork_join_sched_sched_enter,
  .sched_exit      = lithe_fork_join_sched_sched_exit,
  .child_enter     = lithe_fork_join_sched_child_enter,
  .child_exit      = lithe_fork_join_sched_child_exit,
  .context_block   = lithe_fork_join_sched_context_block,
  .context_unblock = lithe_fork_join_sched_context_unblock,
  .context_yield   = lithe_fork_join_sched_context_yield,
  .context_exit    = lithe_fork_join_sched_context_exit
};

static lithe_fork_join_context_t *__ctx_alloc(lithe_fork_join_sched_t *sched,
                                              size_t stacksize)
{
    /* Reuse only from the matching size class (see the comment on
     * context_zombie_class). The size check is belt-and-braces: within a class
     * the usable size varies slightly because of the offset randomisation
     * below, so never trust the class alone. A misfit is dropped rather than
     * re-inserted, which cannot loop. */
    lithe_fork_join_context_t *ctx;
    if (fjs_ctx_pool_bucketed()) {
        int cls = fjs_ctx_size_class(stacksize);
        ctx = wfl_remove(&context_zombie_class[cls]);
        if (ctx && ctx->context.stack.size < stacksize) {
            int ss = ctx->context.stack.size + ctx->stack_offset;
            munmap(ctx->context.stack.bottom, ss);
            ctx = NULL;
        }
    } else {
        ctx = wfl_remove(&context_zombie_list);
    }
    if (!ctx) {
		int offset = ROUNDUP(sizeof(lithe_fork_join_context_t), ARCH_CL_SIZE);
		/* Use the target sched's rseed instead of lithe_sched_current()'s
		 * rseed: the caller (lithe_fork_join_context_create) passes the
		 * sched explicitly, and the calling context may not have *any*
		 * sched entered yet (e.g. PMIx progress-thread start during
		 * MPI_Init, where main has been promoted to vcore 0 but the
		 * OPAL sched has only been registered, not entered). Using
		 * rseed(0) there dereferences a NULL lithe_sched_current() and
		 * crashes. max_vcores() likewise needs vcore_lib_init()
		 * complete, which is the case once the host has called
		 * lithe_ensure_main_on_vcore0(); the caller of context_create is
		 * responsible for that. */
		int mv = max_vcores();
		if (mv < 1) mv = 1;
		offset += rand_r(&rseed_s(sched, 0)) % mv * ARCH_CL_SIZE;
		stacksize = ROUNDUP(stacksize + offset, PGSIZE);
		int prot = PROT_READ|PROT_WRITE;
		if (fjs_stack_exec())
			prot |= PROT_EXEC;
		void *stackbot = mmap(
			0, stacksize, prot,
			MAP_PRIVATE|MAP_ANONYMOUS, -1, 0
		);
		if (stackbot == MAP_FAILED)
			abort();
#ifdef MADV_HUGEPAGE
		if (fjs_stack_hugepage() && stacksize >= (2u << 20))
			(void)madvise(stackbot, stacksize, MADV_HUGEPAGE);
#endif
		ctx = stackbot + stacksize - offset;
		ctx->stack_offset = offset;
		ctx->context.stack.bottom = stackbot;
		ctx->context.stack.size = stacksize - offset;
	}
	return ctx;
}

/* Poison value stamped into a context's start_routine when it is freed, so a
 * later dequeue of a still-enqueued (use-after-free) or doubly-freed context is
 * caught at the point of failure instead of crashing later in __set_tls_desc
 * with a zeroed tls_desc. lithe_fork_join_context_init resets start_routine to
 * the real entry, so this only ever fires on a genuinely-freed context. */
#define FJS_CTX_FREED_POISON ((void (*)(void*))0xDEADBEEFUL)

static void __ctx_free(lithe_fork_join_context_t *ctx)
{
    if (ctx->start_routine == FJS_CTX_FREED_POISON) {
        fprintf(stderr, "[LITHE FJS] FATAL: double free of context %p "
                "(id=%d state=%d). A worker context was destroyed twice "
                "(reaper + self-exit race).\n",
                ctx, ctx->context.id, ctx->state);
        abort();
    }
    ctx->start_routine = FJS_CTX_FREED_POISON;
    if (fjs_ctx_pool_bucketed()) {
        int cls = fjs_ctx_free_class(ctx->context.stack.size);
        if (wfl_size(&context_zombie_class[cls]) < 1000) {
            wfl_insert(&context_zombie_class[cls], ctx);
            return;
        }
    } else if (wfl_size(&context_zombie_list) < 1000) {
        wfl_insert(&context_zombie_list, ctx);
        return;
    }
    {
		int stacksize = ctx->context.stack.size + ctx->stack_offset;
		int ret = munmap(ctx->context.stack.bottom, stacksize);
		assert(!ret);
	}
}

static int get_next_queue_id_for(lithe_fork_join_sched_t *sched)
{
	/* Find the next available core from our list of online cores. */
	int id, next_id;
	while (1) {
		id = sched->next_queue_id;
		next_id = id + 1 == max_vcores() ? 0 : id + 1;
		while (!vconline_s(sched, next_id) && next_id != id)
			next_id = next_id + 1 == max_vcores() ? 0 : next_id + 1;

		if (__sync_bool_compare_and_swap(&sched->next_queue_id, id, next_id))
			return id;
        cmb();
	}
}

static int get_next_queue_id()
{
	lithe_fork_join_sched_t *sched = (void *)lithe_sched_current();
	return get_next_queue_id_for(sched);
}

static inline int fjs_lpt_on(void);
static inline uint64_t fjs_lpt_key(lithe_fork_join_context_t *f);
static inline uint64_t fjs_lpt_now(void);
/* Under the queue lock: keep lpt_ranked_queued = queued contexts with a key. */
static inline void fjs_lpt_count_in(lithe_fork_join_sched_t *sched, lithe_fork_join_context_t *ctx)
{
	if (!fjs_lpt_on())
		return;
	ctx->lpt_enq_ns = fjs_lpt_now();
	/* Count every queued context while any context in this scheduler has a prediction,
	 * so the aging check also runs for unranked ones; a scheduler with no predictions
	 * at all (fine-grained OpenMP) keeps the zero-cost fast path. */
	if (fjs_lpt_key(ctx) > 0 || __atomic_load_n(&sched->lpt_ranked_queued, __ATOMIC_RELAXED) > 0) {
		ctx->lpt_counted = 1;
		__atomic_fetch_add(&sched->lpt_ranked_queued, 1, __ATOMIC_RELAXED);
	}
}
static inline void fjs_lpt_count_out(lithe_fork_join_sched_t *sched, lithe_fork_join_context_t *ctx)
{
	if (ctx->lpt_counted) {
		ctx->lpt_counted = 0;
		__atomic_fetch_sub(&sched->lpt_ranked_queued, 1, __ATOMIC_RELAXED);
	}
}

/* LITHE_FJS_STEAL_DELAY_US (2026-10-21, default 0 = off): another hart may take a
 * queued context (steal, or LPT's global pick) only once it has waited this long in
 * its home queue, so a context woken while its home hart is briefly busy goes back to
 * the hart whose caches hold its data. Hosted MPI+OpenMP 2 ranks x 2 threads: the
 * SpMV kernel ran 24 % more cycles than vanilla on the same work, with OpenMP contexts
 * moving between harts every few regions (GAPS C18). Stealing stays possible after
 * the delay, so nothing starves (LITHE_FJS_STEAL=none + LPT off hung at CG iter 3). */
static uint64_t fjs_steal_delay_ns = (uint64_t)-1;
static inline uint64_t fjs_lpt_now(void);
static inline uint64_t fjs_steal_delay(void)
{
	uint64_t d = __atomic_load_n(&fjs_steal_delay_ns, __ATOMIC_RELAXED);
	if (d == (uint64_t)-1) {
		const char *e = getenv("LITHE_FJS_STEAL_DELAY_US");
		d = e ? (uint64_t)strtoull(e, NULL, 10) * 1000ull : 0;
		__atomic_store_n(&fjs_steal_delay_ns, d, __ATOMIC_RELAXED);
	}
	return d;
}
/* Under tqlock: may a hart other than the home one take this queued context? */
static inline bool fjs_steal_ok(lithe_fork_join_context_t *f)
{
	uint64_t d = fjs_steal_delay();
	return d == 0 || f->lpt_enq_ns == 0 || fjs_lpt_now() - f->lpt_enq_ns >= d;
}

/* LITHE_FJS_WAKE_LOCAL (GAPS B62, default 0). A wake in a child scheduler walked
 * lithe_hart_request up to the root even when the child already owned a hart spinning
 * idle in its hart_enter; across L3/NUMA domains that root<->child ledger and grant
 * traffic was 4x the far-cache fills of the same wake in the root (fanout 17 vs 5.6 us).
 * Spinners register in idle_spinning; a waker claims units (CAS) and requests harts only
 * for the rest. Units are fungible: a spinner leaving the loop takes one back if any is
 * left, and if none is, its unit was claimed, so it restarts to serve the wake. Both
 * sides order through SEQ_CST on idle_spinning after the runnable_count bump. */
static int fjs_wake_local = -1;
static inline int fjs_wake_local_on(void)
{
	int v = __atomic_load_n(&fjs_wake_local, __ATOMIC_RELAXED);
	if (v < 0) {
		const char *e = getenv("LITHE_FJS_WAKE_LOCAL");
		v = (e && *e == '1') ? 1 : 0;
		__atomic_store_n(&fjs_wake_local, v, __ATOMIC_RELAXED);
	}
	return v;
}
/* Claim up to h spinning harts of s; returns how many were claimed. */
static inline int fjs_claim_spinners(lithe_fork_join_sched_t *s, int h)
{
	long n = __atomic_load_n(&s->idle_spinning, __ATOMIC_SEQ_CST);
	while (n > 0) {
		long t = n < h ? n : h;
		if (__atomic_compare_exchange_n(&s->idle_spinning, &n, n - t, false,
		                                __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST))
			return (int)t;
	}
	return 0;
}
/* Spinner leaving: returns 1 if its unit had been claimed (it must serve a wake). */
static inline int fjs_spinner_leave(lithe_fork_join_sched_t *s)
{
	long n = __atomic_load_n(&s->idle_spinning, __ATOMIC_SEQ_CST);
	while (n > 0)
		if (__atomic_compare_exchange_n(&s->idle_spinning, &n, n - 1, false,
		                                __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST))
			return 0;
	return 1;
}

static int __thread_enqueue_on(lithe_fork_join_sched_t *sched,
                               lithe_fork_join_context_t *ctx, bool athead)
{
	assert(ctx->context.sched == (lithe_sched_t *)sched);
	ctx->state = FJS_CTX_RUNNABLE;

	if (ctx->preferred_vcq == -1 || !vconline_s(sched, ctx->preferred_vcq))
		ctx->preferred_vcq = get_next_queue_id_for(sched);
	/* B42: never queue on a vcore blocked in the kernel (nobody would drain it). */
	if (parlib_vcores_blocked() > 0) {
		for (int k = 0; k < (int)max_vcores() && parlib_vcore_blocked(ctx->preferred_vcq); k++)
			ctx->preferred_vcq = get_next_queue_id_for(sched);
	}

	int vcoreid = ctx->preferred_vcq;
	spin_pdr_lock(&tqlock_s(sched, vcoreid));
	if (athead)
		TAILQ_INSERT_HEAD(&tqueue_s(sched, vcoreid), &ctx->context, link);
	else
		TAILQ_INSERT_TAIL(&tqueue_s(sched, vcoreid), &ctx->context, link);
	tqsize_s(sched, vcoreid)++;
	fjs_lpt_count_in(sched, ctx);
	if (fjs_steal_delay() && !fjs_lpt_on())
		ctx->lpt_enq_ns = fjs_lpt_now(); /* queued-since, as LPT records it */
	/* CHANGE 1: bump the O(1) any-runnable indicator under tqlock, so any
	 * vcore that could find this context in the queue also observes count>0. */
	__atomic_fetch_add(&sched->runnable_count, 1, __ATOMIC_SEQ_CST);
	spin_pdr_unlock(&tqlock_s(sched, vcoreid));

	return vcoreid;
}

static int __thread_enqueue(lithe_fork_join_context_t *ctx, bool athead)
{
	assert(lithe_sched_current() == ctx->context.sched);
	return __thread_enqueue_on((lithe_fork_join_sched_t *)ctx->context.sched,
	                           ctx, athead);
}

/* CHANGE 2 (O(1) team broadcast wake): batched wake state.
 *
 * The OpenMP fork/join release wakes all N team worker contexts at one region
 * boundary. Done naively, each lithe_context_unblock -> schedule_context issues
 * its OWN lithe_hart_request(1) -- a walk up the scheduler tree that bottoms out
 * in base_hart_request -> parlib_reactor_wake() (an eventfd write syscall) +
 * maybe_vcore_request -- and its own reactor_wake. Over miniFE CG's thousands
 * of region boundaries this O(N)-per-boundary hart accounting + eventfd-write
 * storm grows with thread count (the `write` count dominated the lithified
 * strace; see HPDC_SUITE.md). The wake-batch lets the release path enqueue all
 * N workers in ONE pass and then issue a SINGLE lithe_hart_request(N) + a single
 * reactor_wake. This is exactly equivalent to N individual unblocks for the
 * parent's hart bookkeeping (the parent sees the same total demand N) but with
 * O(1) tree-walks/syscalls instead of O(N).
 *
 * Thread-local because the entire release loop runs synchronously on the master
 * context (lithe_context_unblock enqueues without switching contexts), so
 * begin/end bracket exactly one uninterrupted enqueue pass on the master's
 * stack. Plain C atomics / lithe primitives only -- no Linux sync above parlib,
 * and it preserves the parent/child hart-grant model (the deferred request is
 * still lithe_hart_request on the current FJ sched, whose parent grants the
 * harts). The depth counter makes a missed/extra end a no-op rather than a hang
 * for the common (non-team) schedule_context path, which never opens a batch and
 * thus keeps its immediate hart_request + reactor_wake. */
static __thread int fjs_wake_batch_depth;
static __thread int fjs_wake_batch_harts;
static __thread int fjs_wake_batch_wake;

static int fjs_clamp_hart_request(lithe_fork_join_sched_t *s, int h);

void lithe_fork_join_wake_batch_begin(void)
{
	if (fjs_wake_batch_depth++ == 0) {
		fjs_wake_batch_harts = 0;
		fjs_wake_batch_wake = 0;
	}
}

void lithe_fork_join_wake_batch_end(void)
{
	if (fjs_wake_batch_depth <= 0) {
		fjs_wake_batch_depth = 0;
		return;
	}
	if (--fjs_wake_batch_depth != 0)
		return;
	int h = fjs_wake_batch_harts;
	int w = fjs_wake_batch_wake;
	lithe_sched_t *cur;
	fjs_wake_batch_harts = 0;
	fjs_wake_batch_wake = 0;
	/* One hart request for the whole team release (parent grants N harts), then
	 * one reactor poke for any vcores parked in parlib_reactor_drive(-1). */
	if (h > 0) {
		int h_req = h;
		cur = lithe_sched_current();
		if (cur != NULL && cur->funcs == &lithe_fork_join_sched_funcs)
			h = fjs_clamp_hart_request((lithe_fork_join_sched_t *)cur, h);
		if (lithe_sched_trace_enabled())
			lithe_sched_trace_emit(LITHE_ST_HART_REQUEST, cur,
			                       (uint32_t)h_req, LITHE_ST_DEC_OK, (int32_t)h);
		if (h > 0)
			lithe_hart_request(h);
	}
	if (w)
		fjs_maybe_reactor_wake();
}

/* Clamp positive hart demand to soft_cap - owned. Returns 0 when at/over
 * cap (caller still enqueues; existing harts must multiplex via yield). */
static int fjs_clamp_hart_request(lithe_fork_join_sched_t *s, int h)
{
	long owned, room;
	int cap;

	if (h <= 0 || s == NULL)
		return h;
	fjs_tradespace_init_once();
	cap = fjs_soft_hart_cap;
	if (cap <= 0)
		return h;
	owned = (long)parlib_atomic_read(&s->sched.harts);
	/* LOST-WAKEUP GUARD: harts committed to yield (leaving_harts) are still
	 * counted in sched.harts until lithe_hart_yield's decrement lands, but
	 * they will never dequeue this enqueue. Treat them as not-owned so the
	 * request is granted instead of clamped to a reactor poke that a fully
	 * yielded hart never sees (single-OS P1K4 nx=80 CG stall). */
	{
		long leaving = __atomic_load_n(&s->leaving_harts, __ATOMIC_SEQ_CST);
		if (leaving > 0)
			owned -= leaving;
		if (owned < 0)
			owned = 0;
	}
	room = (long)cap - owned;
	if (room <= 0) {
		if (fjs_interfere_on())
			__sync_fetch_and_add(&fjs_cnt_hart_cap_clamp, 1);
		return 0;
	}
	if (h > room) {
		if (fjs_interfere_on())
			__sync_fetch_and_add(&fjs_cnt_hart_cap_clamp, 1);
		return (int)room;
	}
	return h;
}

/* Issue (or batch) a hart request + reactor poke. Factored out of
 * schedule_context so the WARM VCORES resume path (lithe_fork_join_resume_warm)
 * shares the exact same batched accounting: while a team-wake batch is open
 * (master's fork/join release loop) the request is deferred and collapsed into
 * a single lithe_hart_request(N) + one reactor_wake at batch end. */
static void fjs_request_harts(int h)
{
	lithe_sched_t *cur;
	int h_req = h;

	if (h > 0) {
		cur = lithe_sched_current();
		if (cur != NULL && cur->funcs == &lithe_fork_join_sched_funcs) {
			h = fjs_clamp_hart_request((lithe_fork_join_sched_t *)cur, h);
			if (h > 0 && fjs_wake_local_on() && fjs_wake_batch_depth <= 0) {
				h -= fjs_claim_spinners((lithe_fork_join_sched_t *)cur, h);
				if (h <= 0)
					return;
			}
			if (lithe_sched_trace_enabled())
				lithe_sched_trace_emit(LITHE_ST_HART_REQUEST, cur,
				                       (uint32_t)h_req, LITHE_ST_DEC_OK, (int32_t)h);
			if (h <= 0) {
				/* At soft cap: wake parked siblings so they notice
				 * the new RUNNABLE and multiplex; do not inflate
				 * root_sched_harts_needed past the rank budget. */
				fjs_maybe_reactor_wake();
				return;
			}
		}
	}
	if (fjs_wake_batch_depth > 0) {
		fjs_wake_batch_harts += h;
		fjs_wake_batch_wake = 1;
		return;
	}
	lithe_hart_request(h);
	/* Wake siblings parked in reactor_drive (incl. timed idle park on
	 * wake_efd when pending==0). Gating only on pending>0 left surplus
	 * vcores yield-thrashing through empty tdequeue/spinlock. */
	fjs_maybe_reactor_wake();
}

void lithe_wake_hint(int vc); /* lithe.c, LITHE_WAKE_HINT */
static unsigned int fjs_home_grace = 256; /* pauses */
int lithe_wake_hint_enabled(void);
static void schedule_context(lithe_fork_join_context_t *ctx, bool athead)
{
	lithe_sched_t *owner;
	lithe_sched_t *cur;

	lithe_wake_hint(__thread_enqueue(ctx, athead));
	/* CHANGE 2: when a team-wake batch is open (master's fork/join release
	 * loop), enqueue all workers first and defer a single hart_request(N) +
	 * reactor_wake to lithe_fork_join_wake_batch_end().
	 *
	 * Cross-sched enqueue (progress ctx on root while caller is a hosted
	 * child FJS): attribute hart demand to the OWNER sched. Otherwise
	 * lithe_hart_request() inflates the caller's harts_needed and the
	 * owner never runs — Allreduce/Finalize livelock under RPH>1. */
	owner = ctx->context.sched;
	cur = lithe_sched_current();
	if (owner != NULL && owner != cur) {
		int h = 1;
		if (owner->funcs == &lithe_fork_join_sched_funcs) {
			h = fjs_clamp_hart_request((lithe_fork_join_sched_t *)owner, 1);
			if (h > 0 && fjs_wake_local_on())
				h -= fjs_claim_spinners((lithe_fork_join_sched_t *)owner, h);
		}
		if (h > 0)
			lithe_hart_request_for(owner, h);
		fjs_maybe_reactor_wake();
	} else {
		fjs_request_harts(1);
	}
	if (lithe_sched_trace_enabled()) {
		lithe_fork_join_sched_t *s =
			(lithe_fork_join_sched_t *)ctx->context.sched;
		lithe_sched_trace_emit(LITHE_ST_PROGRESS_WAKE, s,
		                       (uint32_t)ctx->context.id, LITHE_ST_DEC_OK,
		                       (int32_t)__atomic_load_n(&s->runnable_count,
		                                                 __ATOMIC_RELAXED));
	}
}

/* ===================== WARM VCORES (persistent worker dispatch) ============
 *
 * Problem (measured, HPDC_SUITE.md): after the sticky top FJ sched + O(1)
 * dispatch + O(1) team broadcast wake, lithified hybrid miniFE STILL anti-scales
 * with OMP thread count. The residue is the per-region fork/join round trip --
 * each of N workers, per CG region boundary, cooperatively blocks (condvar +
 * context_block + hart_request(-1) + vcore park) and at the next fork is woken
 * (condvar signal + context_unblock -> runqueue enqueue + hart_request(+1)) and
 * re-dispatched (hart_enter -> dequeue/steal -> context switch). The condvar
 * (mcs locks + TAILQ) and the runqueue enqueue/dequeue are paid per worker per
 * region (~1000+ regions).
 *
 * Warm vcores cut the COUNT of those round-trips' moving parts: a worker that
 * parks at a region boundary records itself in its vcore's warm slot and returns
 * its hart to the parent (so genuinely-idle harts are still released to sibling
 * schedulers -- MPI/PMIx/UCX progress -- preserving composition). The next fork
 * is a single go-flag write per worker + ONE batched hart_request for the whole
 * team; the re-granted hart, on entering hart_enter, finds the worker in its own
 * slot (same vcore => warm cache/TLS) and RESUMES IT IN PLACE -- no condvar, no
 * runqueue enqueue/dequeue, no work-steal scan, no per-worker hart_request.
 *
 * This replicates vanilla's hot-team (1:1 worker<->core, go-flag wake) within
 * the Lithe model while preserving the per-runtime FJ scheduler and the
 * parent/child hart-grant tree (lithe-multi-runtime-schedulers). It uses only
 * C atomics + lithe primitives (no Linux sync above parlib). It does NOT hoard
 * harts (park still returns the hart), so it is not the banned hot-spin /
 * KMP_BLOCKTIME=infinite behavior. */

/* Try to claim the warm worker (if any, and marked GO) parked on vcore `v`.
 * Returns the context to run, or NULL. Atomic CAS of the slot to NULL resolves
 * the race between two harts both scanning the same slot. */
static lithe_fork_join_context_t *fjs_warm_try_slot(lithe_fork_join_sched_t *sched,
                                                    int v)
{
	lithe_context_t *c = __atomic_load_n(&sched->vc_mgmt[v].warm_ctx,
	                                     __ATOMIC_ACQUIRE);
	if (c == NULL)
		return NULL;
	lithe_fork_join_context_t *ctx = (lithe_fork_join_context_t *)c;
	if (!__atomic_load_n(&ctx->warm_go, __ATOMIC_ACQUIRE))
		return NULL;  /* parked but not yet released by a fork */
	lithe_context_t *expected = c;
	if (!__atomic_compare_exchange_n(&sched->vc_mgmt[v].warm_ctx, &expected,
	                                 NULL, false, __ATOMIC_ACQ_REL,
	                                 __ATOMIC_RELAXED))
		return NULL;  /* another hart claimed it first */
	__atomic_store_n(&ctx->warm_state, FJS_WARM_NONE, __ATOMIC_RELEASE);
	ctx->warm_go = 0;
	ctx->warm_vcq = -1;
	ctx->warm_sched = NULL;
	__atomic_fetch_sub(&sched->warm_ready_count, 1, __ATOMIC_SEQ_CST);
	return ctx;
}

/* Find a GO warm worker to resume on this hart: own vcore's slot first (keeps
 * the worker on the same vcore => warm cache/TLS, the whole point), then a scan
 * so a worker is never stranded if a different vcore happened to get the hart
 * (liveness). The scan is bounded by max_vcores() and only runs when the own
 * slot has nothing ready; in the steady state (1 worker per vcore, N harts
 * re-granted for N workers) the own-slot hit fires and the scan is skipped. */
static lithe_fork_join_context_t *fjs_warm_claim(lithe_fork_join_sched_t *sched,
                                                 int my)
{
	lithe_fork_join_context_t *w;
	if (my >= 0 && my < max_vcores()) {
		w = fjs_warm_try_slot(sched, my);
		if (w)
			return w;
	}
	int n = max_vcores();
	for (int i = 0; i < n; i++) {
		if (i == my)
			continue;
		w = fjs_warm_try_slot(sched, i);
		if (w)
			return w;
	}
	return NULL;
}

/* Block callback (runs in vcore context, AFTER the FJS context_block has set
 * state=BLOCKED and done hart_request(-1)). Record the worker in its vcore's
 * warm slot; on slot conflict fall back to the runqueue (OVERFLOW). Then unlock
 * the caller-held mutex -- mirrors lithe_condvar_wait's atomic
 * enqueue-then-unlock, giving the same no-lost-wakeup guarantee against a
 * concurrent resume_warm (which takes the same mutex). */
static void warm_park_cb(lithe_context_t *c, void *arg)
{
	lithe_fork_join_sched_t *sched = (lithe_fork_join_sched_t *)c->sched;
	lithe_fork_join_context_t *ctx = (lithe_fork_join_context_t *)c;
	lithe_mutex_t *mx = (lithe_mutex_t *)arg;
	int v = vcore_id();

	ctx->warm_go = 0;
	lithe_context_t *expected = NULL;
	if (v >= 0 && v < max_vcores() &&
	    __atomic_compare_exchange_n(&sched->vc_mgmt[v].warm_ctx, &expected, c,
	                                false, __ATOMIC_RELEASE, __ATOMIC_RELAXED)) {
		ctx->warm_vcq = v;
		ctx->warm_sched = sched;  /* the sched this slot lives in (migration check) */
		__atomic_store_n(&ctx->warm_state, FJS_WARM_SLOT, __ATOMIC_RELEASE);
	} else {
		/* Slot busy (another worker is already parked on this vcore) or no
		 * valid vcore id: fall back to the proven runqueue path. resume_warm
		 * will re-enqueue via schedule_context. */
		ctx->warm_vcq = -1;
		ctx->warm_sched = NULL;
		__atomic_store_n(&ctx->warm_state, FJS_WARM_OVERFLOW, __ATOMIC_RELEASE);
	}
	if (mx)
		lithe_mutex_unlock(mx);
}

void lithe_fork_join_park_warm(void *mx)
{
	if (lithe_sched_trace_enabled()) {
		lithe_context_t *c = lithe_context_self();
		lithe_sched_trace_emit(LITHE_ST_WARM_PARK,
		                       c ? c->sched : NULL,
		                       c ? (uint32_t)c->id : 0,
		                       LITHE_ST_DEC_OK, 0);
	}
	/* lithe_context_block runs the FJS context_block (state=BLOCKED,
	 * hart_request(-1)) then warm_park_cb, then deschedules the worker. */
	lithe_context_block(warm_park_cb, mx);
	/* Resumed in place by hart_enter (FJS_WARM_SLOT) or via the runqueue
	 * (FJS_WARM_OVERFLOW). Re-lock the mutex to mirror lithe_condvar_wait. */
	if (mx)
		lithe_mutex_lock((lithe_mutex_t *)mx);
}

void lithe_fork_join_resume_warm(lithe_context_t *c)
{
	lithe_fork_join_sched_t *sched = (lithe_fork_join_sched_t *)c->sched;
	lithe_fork_join_context_t *ctx = (lithe_fork_join_context_t *)c;

	int st = __atomic_load_n(&ctx->warm_state, __ATOMIC_ACQUIRE);
	if (st == FJS_WARM_NONE)
		return;  /* not parked-warm (it saw the go-flag and never blocked) */

	int expected_go;
	/* MIGRATION CHECK (lithified libomp re-homes workers between fork-join
	 * schedulers: a worker parks its warm slot in the sched it was running in,
	 * then gets migrated to the sticky top sched before the next fork). The
	 * in-place fast path is only valid when the slot lives in the context's
	 * CURRENT sched -- that sched's hart_enter scans that sched's vc_mgmt and
	 * resume_warm bumps that sched's warm_ready_count. If warm_sched differs the
	 * slot is stranded in a (now defunct) sched whose hart_enter never runs the
	 * claim; using the fast path there hangs forever. So: in-place only when
	 * warm_sched == current sched; otherwise clear the stranded slot and resume
	 * through the proven runqueue on the current sched. */
	bool in_place = (st == FJS_WARM_SLOT && ctx->warm_sched == (void *)sched);
	if (in_place) {
		/* In-place resume. The worker becomes claimable when warm_go==1, so the
		 * ready count MUST be incremented BEFORE warm_go is published, otherwise
		 * a concurrent hart_enter (woken for a sibling worker) could claim this
		 * worker and decrement a count we have not yet incremented -> drift.
		 * We tentatively bump the count, then CAS go 0->1 for idempotency
		 * (resume may be called twice for a target that blocked once); on a lost
		 * CAS we undo the tentative bump. hart_enter then finds it in its vcore
		 * slot -- no enqueue, no steal, no per-worker condvar. */
		__atomic_fetch_add(&sched->warm_ready_count, 1, __ATOMIC_SEQ_CST);
		expected_go = 0;
		if (!__atomic_compare_exchange_n(&ctx->warm_go, &expected_go, 1, false,
		                                 __ATOMIC_ACQ_REL, __ATOMIC_RELAXED)) {
			__atomic_fetch_sub(&sched->warm_ready_count, 1, __ATOMIC_SEQ_CST);
			return;
		}
		fjs_request_harts(1);
	} else {
		/* OVERFLOW, or SLOT stranded by migration: resume through the normal
		 * runqueue (proven, migration-safe path). Idempotent via CAS go 0->1. */
		expected_go = 0;
		if (!__atomic_compare_exchange_n(&ctx->warm_go, &expected_go, 1, false,
		                                 __ATOMIC_ACQ_REL, __ATOMIC_RELAXED))
			return;
		/* If this was a SLOT parked in a different sched, remove the stranded
		 * back-pointer so the defunct sched never holds a dangling context (it
		 * is gated behind that sched's warm_ready_count, which we did not bump,
		 * so it would not be claimed -- but clearing keeps the slot clean for
		 * reuse and teardown). warm_go is already CAS'd to 1, but the stale slot
		 * is in warm_sched (not `sched`), and claims there require that sched's
		 * count>0 which is untouched, so no double-run is possible. */
		if (st == FJS_WARM_SLOT && ctx->warm_sched != NULL &&
		    ctx->warm_vcq >= 0) {
			lithe_fork_join_sched_t *osched =
				(lithe_fork_join_sched_t *)ctx->warm_sched;
			lithe_context_t *exp = c;
			__atomic_compare_exchange_n(&osched->vc_mgmt[ctx->warm_vcq].warm_ctx,
			                            &exp, NULL, false, __ATOMIC_ACQ_REL,
			                            __ATOMIC_RELAXED);
			ctx->warm_vcq = -1;
			ctx->warm_sched = NULL;
		}
		__atomic_store_n(&ctx->warm_state, FJS_WARM_NONE, __ATOMIC_RELEASE);
		schedule_context(ctx, false);
	}
}

/* LPT dispatch (2026-10-19, GAPS B27). Bulk-synchronous rank contexts become
 * runnable together (an allreduce releases them all) and the hart order they are
 * picked in decides the makespan: 4 harts, 4 contexts of 5.2 ms and 4 of 2.7 ms
 * finish in 7.9 ms if picked all-long-first (or all-short-first) and in ~10.4 ms
 * when interleaved -- the measured hosted gap against AMPI, whose static block
 * placement happens to pair one long and one short rank per core. With
 * LITHE_FJS_LPT=1 a hart takes, among the first LPT_SCAN contexts of a queue,
 * the one whose last long run slice was longest (longest processing time first,
 * learned from the previous iteration, no hints). Slices under 100 us (MPI wait
 * polls, yields) do not update the prediction. Default on (see fjs_lpt_on). */
#define FJS_LPT_SCAN 16
#define FJS_LPT_MIN_NS 100000ull
static int fjs_lpt = -1;
static inline int fjs_lpt_on(void)
{
	if (__builtin_expect(fjs_lpt < 0, 0)) {
		/* Default ON since 2026-10-19: lpt_ranked_queued and the gap gate
		 * (fjs_lpt_scan_ok) keep the scan off fine-grained work (nested OpenMP,
		 * region cost, hosted MPI+OpenMP unchanged) and the HPDC login suite (7
		 * apps, NP=2 OMP=4, 3 reps) is neutral; hosted ranks gain 7-12 %.
		 * LITHE_FJS_LPT=0 disables. */
		const char *e = getenv("LITHE_FJS_LPT");
		fjs_lpt = !(e && e[0] == '0');
	}
	return fjs_lpt;
}
static inline uint64_t fjs_lpt_now(void)
{
	struct timespec t;
	clock_gettime(CLOCK_MONOTONIC, &t);
	return (uint64_t)t.tv_sec * 1000000000ull + (uint64_t)t.tv_nsec;
}
static inline void fjs_lpt_start(lithe_fork_join_context_t *ctx)
{
	if (fjs_lpt_on()) {
		ctx->lpt_start_ns = fjs_lpt_now();
	}
}
static inline void fjs_lpt_record(lithe_fork_join_context_t *ctx, uint64_t d)
{
	ctx->lpt_pred_ns = d;
	ctx->lpt_polling = 0;
	lithe_fork_join_sched_t *s = (lithe_fork_join_sched_t *)ctx->context.sched;
	uint64_t hi = __atomic_load_n(&s->lpt_hi_ns, __ATOMIC_RELAXED);
	uint64_t lo = __atomic_load_n(&s->lpt_lo_ns, __ATOMIC_RELAXED);
	hi -= hi >> 5;
	lo = lo ? lo + (lo >> 5) : d;
	__atomic_store_n(&s->lpt_hi_ns, d > hi ? d : hi, __ATOMIC_RELAXED);
	__atomic_store_n(&s->lpt_lo_ns, d < lo ? d : lo, __ATOMIC_RELAXED);
}
/* A context that YIELDS after a short slice is polling (an MPI wait loop): if it
 * kept its long-burst priority, LPT would hand the hart straight back to the poller
 * (trace: a heavy rank re-dequeued every 7 us for 2.5 ms while short ranks with real
 * work queued). Polling contexts rank last until they run a long slice or block. */
static inline void fjs_lpt_stop(lithe_fork_join_context_t *ctx, int yielded)
{
	if (fjs_lpt_on() && ctx->lpt_start_ns) {
		uint64_t d = fjs_lpt_now() - ctx->lpt_start_ns;
		if (d >= FJS_LPT_MIN_NS) {
			fjs_lpt_record(ctx, d);
		} else {
			ctx->lpt_polling = yielded;
		}
	}
}
/* AGING (2026-10-19): without it a context with no prediction could wait forever while
 * others with predictions kept being runnable -- StarPU workers + an OpenMP team in one
 * process hung (LITHE_FJS_LPT=0 ran it in 0.05 s). A context queued longer than
 * FJS_LPT_AGE_NS outranks every prediction, oldest first. */
#define FJS_LPT_AGE_NS 2000000ull
/* AGE SCALING (2026-10-19): a fixed 2 ms age undid LPT whenever bursts are longer
 * than that. Hosted ranks trace (K=8 alt, 4 harts): a 5.3 ms rank woken at 1.72 ms
 * waited until 6.96 ms because at 4.3 ms it and two 2.6 ms ranks had all queued
 * > 2 ms, and the oldest (by 20-30 us) won -- plain FIFO, ~2.6 ms lost per
 * iteration. A context now ages after max(2 ms, M x the scheduler's recent
 * longest burst), M = LITHE_FJS_LPT_AGE_MULT (default 1; 0 = fixed 2 ms).
 * Only predicted contexts get the scaled age (see below). K=8 alt 10.3-10.5 ->
 * 9.3 ms, block 9.3-9.7 -> 9.1-9.4, K=16 alt 17.8-18.0 -> 16.8-17.4; M=2/4 no
 * better. NEGATIVE (same day): ending a burst prediction at the first wait-loop
 * call (lithe_fork_join_should_yield_to_runnable), so spin-waits do not count as
 * compute, measured no better than this alone; removed. */
static int fjs_lpt_age_mult = -1;
static int fjs_lpt_age_all = -1; /* LITHE_FJS_LPT_AGE_ALL=1: scale every context's age (A/B) */
static inline int getenv_lpt_age_all(void)
{
	if (__builtin_expect(fjs_lpt_age_all < 0, 0)) {
		const char *e = getenv("LITHE_FJS_LPT_AGE_ALL");
		fjs_lpt_age_all = e && e[0] == '1';
	}
	return fjs_lpt_age_all;
}
static inline uint64_t fjs_lpt_age_ns(lithe_fork_join_context_t *f)
{
	if (__builtin_expect(fjs_lpt_age_mult < 0, 0)) {
		const char *e = getenv("LITHE_FJS_LPT_AGE_MULT");
		fjs_lpt_age_mult = e ? atoi(e) : 1;
	}
	if (fjs_lpt_age_mult == 0)
		return FJS_LPT_AGE_NS;
	/* only contexts with a long-burst prediction compete on burst length;
	 * unpredicted and polling ones (progress engines) keep the 2 ms age:
	 * scaling theirs cost hosted GPU ranks 2.5-4 % (ROCr progress contexts). */
	if (getenv_lpt_age_all() == 0 && (f->lpt_polling || f->lpt_pred_ns == 0))
		return FJS_LPT_AGE_NS;
	lithe_fork_join_sched_t *s = (lithe_fork_join_sched_t *)f->context.sched;
	uint64_t a = __atomic_load_n(&s->lpt_hi_ns, __ATOMIC_RELAXED) * (uint64_t)fjs_lpt_age_mult;
	return a > FJS_LPT_AGE_NS ? a : FJS_LPT_AGE_NS;
}
static inline uint64_t fjs_lpt_key(lithe_fork_join_context_t *f)
{
	if (f->lpt_enq_ns) {
		uint64_t now = fjs_lpt_now();
		if (now > f->lpt_enq_ns && now - f->lpt_enq_ns > fjs_lpt_age_ns(f))
			return (1ull << 62) + (now - f->lpt_enq_ns);
	}
	return f->lpt_polling ? 0 : f->lpt_pred_ns;
}

/* GAP GATE (2026-10-19): LPT cost nested OpenMP 4-12 % (Lithe libgomp 4x4 128 ->
 * 135 us, 8x4 257 -> 280): its 0.1-0.8 ms chunks earn predictions, so dispatches
 * scanned and locked every queue, yet near-equal bursts gain nothing from reordering
 * (a never-scan arm matched LPT off, so the cost is the scan, not the timestamps).
 * The scan runs only while the decaying max and min of recent long bursts differ by
 * >= LITHE_FJS_LPT_GAP_US (default 1000; 0 = always scan). Measured hi-lo: nested
 * 0.2-0.35 ms, hosted ranks 3.7-6 ms. A ratio gate did not separate them (nested
 * 1.7-3x, ranks 3.5-40x: one 120 us halo slice sets lo). Skipping the scan is plain
 * FIFO, so aging is not needed then. */
static long fjs_lpt_gap_ns = -1;
static inline int fjs_lpt_scan_ok(lithe_fork_join_sched_t *s)
{
	if (__builtin_expect(fjs_lpt_gap_ns < 0, 0)) {
		const char *e = getenv("LITHE_FJS_LPT_GAP_US");
		fjs_lpt_gap_ns = (e ? atol(e) : 1000) * 1000;
	}
	if (fjs_lpt_gap_ns == 0)
		return 1;
	uint64_t hi = __atomic_load_n(&s->lpt_hi_ns, __ATOMIC_RELAXED);
	uint64_t lo = __atomic_load_n(&s->lpt_lo_ns, __ATOMIC_RELAXED);
	return hi >= lo + (uint64_t)fjs_lpt_gap_ns;
}

/* LITHE_FJS_YIELD_STEAL (default ON since 2026-10-22; =0 disables): a context that yields stays runnable and is
 * re-queued on its own hart, so that hart's queue is never empty and the hart never steals: it
 * re-dispatches the yielder (an OpenMP thread spinning at a barrier) while a never-started worker
 * waits in another hart's queue. LAMMPS, 4 OpenMP threads on 2 harts per rank: ~310k sub-2 us
 * re-dispatches of the master per hart, ~1.1k steal attempts in total, 250 vs BOLT 314
 * timesteps/s. With the knob, when the local pick is the context that just yielded on this hart,
 * steal first and fall back to the yielder. Was default off for one SIGSEGV in joblib-nested
 * scikit-learn; that was libomp's reaper race (GAPS B41), reproduced without this knob and fixed.
 * Neutral on every other app, the AI modes and hosted ranks (GAPS C19). */
#define FJS_JY_MAX_VC 1024
static void *fjs_just_yielded[FJS_JY_MAX_VC];
static int fjs_yield_steal = -1;
static inline int fjs_yield_steal_on(void)
{
	if (__builtin_expect(fjs_yield_steal < 0, 0)) {
		const char *e = getenv("LITHE_FJS_YIELD_STEAL");
		fjs_yield_steal = !(e && e[0] == '0');
	}
	return fjs_yield_steal;
}

static lithe_fork_join_context_t *__thread_dequeue()
{
	lithe_fork_join_sched_t *cur = (lithe_fork_join_sched_t *)lithe_sched_current();

	inline lithe_fork_join_context_t *tdequeue(int vcoreid)
	{
		lithe_fork_join_context_t *ctx = NULL;
		if (tqsize(vcoreid)) {
			spin_pdr_lock(&tqlock(vcoreid));
			ctx = (lithe_fork_join_context_t*) TAILQ_FIRST(&tqueue(vcoreid));
			if (ctx && fjs_lpt_on() &&
			    __atomic_load_n(&cur->lpt_ranked_queued, __ATOMIC_RELAXED) > 0 &&
			    fjs_lpt_scan_ok(cur)) {
				lithe_context_t *c = TAILQ_NEXT(&ctx->context, link);
				for (int k = 1; c && k < FJS_LPT_SCAN; k++, c = TAILQ_NEXT(c, link)) {
					lithe_fork_join_context_t *f = (lithe_fork_join_context_t *)c;
					if (fjs_lpt_key(f) > fjs_lpt_key(ctx))
						ctx = f;
				}
			}
			if (ctx) {
				TAILQ_REMOVE(&tqueue(vcoreid), &ctx->context, link);
				fjs_lpt_count_out(cur, ctx);
				tqsize(vcoreid)--;
				/* CHANGE 1: drop the O(1) any-runnable indicator under tqlock. */
				__atomic_fetch_sub(&cur->runnable_count, 1, __ATOMIC_SEQ_CST);
			}
			spin_pdr_unlock(&tqlock(vcoreid));
		}
		if (ctx)
			ctx->preferred_vcq = vcore_id();
		return ctx;
	}

	/* LPT across ALL queues (B27): picking the longest burst only within the local
	 * queue left a long context queued behind another on one hart while a free
	 * hart ran short ones from its own queue (trace: the heavy rank waited 5 ms
	 * and set the iteration time). Peek every non-empty queue, then remove the
	 * winner under its queue lock if it is still there; ties go to the local
	 * queue. One lock per non-empty queue per dispatch: opt-in only. */
	inline lithe_fork_join_context_t *lpt_pick_global(void)
	{
		int me = vcore_id(), mv = (int)max_vcores(), bv = -1;
		lithe_fork_join_context_t *best = NULL;
		for (int i = 0; i < mv; i++) {
			int v = (me + i) % mv;
			if (tqsize(v) == 0)
				continue;
			spin_pdr_lock(&tqlock(v));
			lithe_context_t *c = TAILQ_FIRST(&tqueue(v));
			for (int k = 0; c && k < FJS_LPT_SCAN; k++, c = TAILQ_NEXT(c, link)) {
				lithe_fork_join_context_t *f = (lithe_fork_join_context_t *)c;
				if (v != me && !fjs_steal_ok(f))
					continue;
				if (!best || fjs_lpt_key(f) > fjs_lpt_key(best)) {
					best = f;
					bv = v;
				}
			}
			spin_pdr_unlock(&tqlock(v));
		}
		if (!best)
			return NULL;
		lithe_fork_join_context_t *got = NULL;
		spin_pdr_lock(&tqlock(bv));
		lithe_context_t *c = TAILQ_FIRST(&tqueue(bv));
		for (int k = 0; c && k < FJS_LPT_SCAN; k++, c = TAILQ_NEXT(c, link)) {
			if ((lithe_fork_join_context_t *)c == best) {
				TAILQ_REMOVE(&tqueue(bv), c, link);
				fjs_lpt_count_out(cur, best);
				tqsize(bv)--;
				__atomic_fetch_sub(&cur->runnable_count, 1, __ATOMIC_SEQ_CST);
				got = best;
				break;
			}
		}
		spin_pdr_unlock(&tqlock(bv));
		if (got)
			got->preferred_vcq = me;
		return got;
	}

	int vcoreid = vcore_id();
	lithe_fork_join_context_t *ctx = NULL;
	int local_tq;

	/* ORPHANED QUEUES (B42, with PARLIB_SPARE_VCORES): a vcore whose OS thread is
	 * asleep in the kernel under a uthread (main in pthread_join) still owns a run
	 * queue nobody drains -- its hart is gone, and the compensating hart's own queue
	 * never empties (barrier spinners re-queue there), so it never stole: the queued
	 * workers starved and their foreign masters waited forever. Drain such queues
	 * first. One load when nothing is blocked. */
	if (parlib_vcores_blocked() > 0) {
		int mv = (int)max_vcores();
		for (int v = 0; v < mv && !ctx; v++)
			if (v != vcoreid && tqsize(v) && parlib_vcore_blocked(v))
				ctx = tdequeue(v);
	}

	/* Try and grab a thread from our queue */
	local_tq = tqsize(vcoreid);
	if (!ctx && fjs_lpt_on() && __atomic_load_n(&cur->lpt_ranked_queued, __ATOMIC_RELAXED) > 0 &&
	    fjs_lpt_scan_ok(cur))
		ctx = lpt_pick_global();
	if (!ctx)
		ctx = tdequeue(vcoreid);
	if (ctx && lithe_sched_trace_enabled()) {
		lithe_sched_trace_emit(LITHE_ST_DEQUEUE_LOCAL, cur,
		                       (uint32_t)ctx->context.id, LITHE_ST_DEC_OK,
		                       (int32_t)__atomic_load_n(&cur->runnable_count,
		                                                 __ATOMIC_RELAXED));
	}

	/* Tradespace: none/local => never steal from other vcores on this sched. */
	fjs_tradespace_init_once();
	if (fjs_steal_mode != LITHE_FJS_STEAL_DEFAULT)
		return ctx;

	lithe_fork_join_context_t *yielder = NULL;
	if (ctx && vcoreid >= 0 && vcoreid < FJS_JY_MAX_VC &&
	    fjs_just_yielded[vcoreid] == (void *)ctx) {
		fjs_just_yielded[vcoreid] = NULL;
		if (fjs_yield_steal_on() &&
		    __atomic_load_n(&cur->runnable_count, __ATOMIC_ACQUIRE) > 0) {
			yielder = ctx;
			ctx = NULL;
		}
	}

	/* If there isn't one, try and steal one from someone else's queue.
	 *
	 * CHANGE 1 (O(1) dispatch): first consult the global runnable indicator.
	 * If it is 0, NO per-vcore queue on this scheduler holds a runnable
	 * context, so the O(max_vcores()) power-of-two + full scan below cannot
	 * find anything -- skip it and return NULL so the caller yields the hart
	 * immediately. This is the structural fix for the per-region-boundary cost
	 * that grew with thread count: at a join/barrier the N woken vcores that
	 * drained their queues used to EACH scan all max_vcores() queues fruitlessly
	 * (O(N*vcores) per boundary); now each does one atomic load. Correctness:
	 * every enqueue increments the count under tqlock before its paired
	 * lithe_hart_request (schedule_context), so a vcore that observes 0 is
	 * guaranteed that the hart granted for that request will run a later
	 * hart_enter which observes the post-increment value -- no lost wakeup. */
	if (!ctx) {
		if (__atomic_load_n(&cur->runnable_count, __ATOMIC_ACQUIRE) <= 0)
			return yielder;

		/* Steal up to half of the threads in the queue and return the first */
		lithe_fork_join_context_t *steal_threads(int vcoreid)
		{
			lithe_fork_join_context_t *ctx = NULL;
			int num_to_steal = (tqsize(vcoreid) + 1) / 2;
			if (num_to_steal && fjs_steal_delay()) {
				lithe_context_t *h;
				bool ok;
				spin_pdr_lock(&tqlock(vcoreid));
				h = TAILQ_FIRST(&tqueue(vcoreid));
				ok = h && fjs_steal_ok((lithe_fork_join_context_t *)h);
				spin_pdr_unlock(&tqlock(vcoreid));
				if (!ok)
					return NULL;
			}
			if (num_to_steal) {
				ctx = tdequeue(vcoreid);
				if (ctx) {
					for (int i=1; i<num_to_steal; i++) {
						lithe_fork_join_context_t *u = tdequeue(vcoreid);
						if (u) __thread_enqueue(u, false);
						else break;
					}
				}
			}
			return ctx;
		}

		if (lithe_sched_trace_enabled()) {
			uint8_t dec = LITHE_ST_DEC_OK;
			/* Incorrect if we steal while our local queue was nonempty. */
			if (local_tq > 0)
				dec |= LITHE_ST_DEC_STEAL_LOCAL_NONEMPTY;
			lithe_sched_trace_emit(LITHE_ST_STEAL_ATTEMPT, cur, 0, dec,
			                       (int32_t)local_tq);
		}

		/* First try doing power of two choices. Prefer sampling within
		 * max_vcores(); under HOSTED_FULLCORE num_vcores()≈max and most
		 * queues are empty — PoT still helps when a peer queue is hot. */
		int span = (int)max_vcores();
		if (span < 1)
			span = 1;
		if (fjs_steal_topo && vcoreid >= 0 && vcoreid < FJS_TOPO_MAX_VC) {
			/* A6: nearest-first walk; only lock nonempty queues. */
			if (!__atomic_load_n(&fjs_topo_ready[vcoreid], __ATOMIC_ACQUIRE))
				fjs_topo_build(vcoreid, span);
			for (int k = 0; k < fjs_topo_n[vcoreid] && !ctx; k++) {
				int v = fjs_topo_order[vcoreid][k];
				if (tqsize_s(cur, v) > 0)
					ctx = steal_threads(v);
			}
		}
		int choice[2] = { rand_r(&rseed(vcoreid)) % span,
		                  rand_r(&rseed(vcoreid)) % span};
		int size[2] = { ctx ? 0 : tqsize_s(cur, choice[0]),
		                ctx ? 0 : tqsize_s(cur, choice[1])};
		int id = (size[0] > size[1]) ? 0 : 1;
		if (!ctx && size[id] > 0 && vcoreid != choice[id])
			ctx = steal_threads(choice[id]);
		else if (!ctx && size[!id] > 0 && vcoreid != choice[!id])
			ctx = steal_threads(choice[!id]);

		/* Fall back: walk all vcores but only lock/dequeue nonempty
		 * queues. Calling steal_threads on 90+ empty HOSTED_FULLCORE
		 * queues was pure FJS idle tax. Do NOT filter on vconline alone
		 * (that missed preferred_vcq work and blew P1K2 to ~0.5ms). */
		if (!ctx) {
			int mv = (int)max_vcores();
			int i = (vcoreid + 1) % mv;
			while (i != vcoreid) {
				if (tqsize_s(cur, i) > 0) {
					ctx = steal_threads(i);
					if (ctx) break;
				}
				i = (i + 1) % mv;
			}
		}

		if (lithe_sched_trace_enabled()) {
			if (ctx)
				lithe_sched_trace_emit(LITHE_ST_STEAL_SUCCESS, cur,
				                       (uint32_t)ctx->context.id,
				                       LITHE_ST_DEC_OK,
				                       (int32_t)ctx->preferred_vcq);
			else
				lithe_sched_trace_emit(LITHE_ST_STEAL_FAIL, cur, 0,
				                       LITHE_ST_DEC_OK,
				                       (int32_t)__atomic_load_n(
					                       &cur->runnable_count,
					                       __ATOMIC_RELAXED));
		}
	}
	if (yielder) {
		if (ctx)
			__thread_enqueue_on(cur, yielder, false);  /* stays on this hart's queue */
		else
			ctx = yielder;
	}
	return ctx;
}

lithe_fork_join_sched_t *lithe_fork_join_sched_create()
{
  /* Allocate all the scheduler data together. */
  struct sched_data {
    lithe_fork_join_sched_t sched;
    lithe_fork_join_context_t main_context;
    struct lithe_fork_join_vc_mgmt vc_mgmt[];
  };

  /* Use a zombie list to reuse old schedulers if available, otherwise, create
   * a new one. */
  struct sched_data *s = wfl_remove(&sched_zombie_list);
  if (!s) {
    s = parlib_aligned_alloc(PGSIZE,
            sizeof(*s) + sizeof(struct lithe_fork_join_vc_mgmt) * max_vcores());
    s->sched.vc_mgmt = &s->vc_mgmt[0];
    s->sched.sched.funcs = &lithe_fork_join_sched_funcs;
  }

  /* Initialize the scheduler. */
  lithe_fork_join_sched_init(&s->sched, &s->main_context);
  return &s->sched;
}

/* ---------------------------------------------------------------------------
 * General, runtime-combination-agnostic root fork-join scheduler bootstrap.
 *
 * The lithified stack composes several runtimes (libomp, OPAL/MPI, OpenPMIx,
 * lithified UCX/OFI). Whichever one initializes FIRST and needs a schedulable
 * lithe context calls lithe_ensure_root_fork_join_sched(); it returns a single
 * process-wide root fork-join scheduler that is entered under the base
 * scheduler. Later runtimes call the same function, find the SAME root, and
 * compose their own child schedulers under it via the normal lithe parent/child
 * hart-grant tree (see lithe-multi-runtime-schedulers and the
 * lithe-bootstrap-any-runtime-combination rule).
 *
 * The base bootstrap (main thread -> uthread on vcore 0, parlib vcore pool,
 * base scheduler) is owned here in lithe/parlib via lithe_ensure_main_on_vcore0
 * -- NOT by any single higher runtime such as OPAL/MPI. That is what makes the
 * pure-OpenMP (no MPI), MPI-only, and OMP+MPI combinations all work with the
 * same code path: the bootstrap is triggered by need, happens exactly once, and
 * is idempotent if multiple runtimes try to ensure it.
 *
 * Concurrency: in every real scenario the root is created and entered during
 * single-threaded process init on the main uthread (the very thread that ran
 * the .so constructors). lithe_sched_enter() must run on a uthread (not in
 * vcore context); the first caller is always the master/main thread before any
 * worker context exists, and after lithe_root_fj_entered is set later callers
 * (which may be on worker vcores) only read the cached pointer. The creation
 * CAS guards against a duplicate root if two runtimes race the very first call.
 * ------------------------------------------------------------------------- */
static lithe_fork_join_sched_t *lithe_root_fj_sched;
static int lithe_root_fj_entered;
/* 1 when the root was ADOPTED from the caller's current scheduler rather than
 * created here. An adopted root must never be exited or destroyed by lithe --
 * it belongs to the application. */
static int lithe_root_fj_adopted;

/* The root fork-join scheduler if one exists, without bootstrapping one (safe from a
 * foreign thread). */
lithe_fork_join_sched_t *lithe_root_fork_join_sched_if_any(void)
{
  return __atomic_load_n(&lithe_root_fj_sched, __ATOMIC_ACQUIRE);
}

/* FOREIGN FIRST CALLER (B35, 2026-10-22). The base bootstrap turns its CALLER into thread0:
 * parlib's vcore_request_init moves the caller's context onto vcore 0's pthread and parks the
 * caller's OS thread forever. For the main thread that is the design (main's context continues on
 * vcore 0; exit() ends the process). For any other pthread -- a Python thread making the process's
 * first OpenMP call -- its start routine then returns on vcore 0's thread and pthread_join on it
 * never returns (both Lithe runtimes hung). So a non-main first caller does not bootstrap itself:
 * a lithe-internal helper pthread does, becomes thread0, enters the root, and parks its context
 * forever as a BLOCKED context (it holds no hart). The caller stays a plain foreign thread and
 * proceeds as one (B34). The root is never exited, so the parked context is never waited on.
 * LITHE_FOREIGN_BOOTSTRAP=0 restores the old hijack (A/B). */
lithe_fork_join_sched_t *lithe_ensure_root_fork_join_sched(void);
static pthread_mutex_t fjs_fb_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t fjs_fb_cond = PTHREAD_COND_INITIALIZER;
static int fjs_fb_state;  /* 0 none, 1 helper running, 2 done */
static __thread int fjs_fb_is_helper;

static void fjs_fb_park(lithe_context_t *ctx, void *arg)
{
  (void)ctx; (void)arg;  /* never unblocked */
}

static void *fjs_fb_helper(void *arg)
{
  (void)arg;
  fjs_fb_is_helper = 1;
  lithe_ensure_root_fork_join_sched();
  int became_thread0 = current_uthread != NULL;
  pthread_mutex_lock(&fjs_fb_lock);
  fjs_fb_state = 2;
  pthread_cond_broadcast(&fjs_fb_cond);
  pthread_mutex_unlock(&fjs_fb_lock);
  if (!became_thread0)
    return NULL;  /* the main thread won the bootstrap race; nothing to hold */
  for (;;)
    lithe_context_block(fjs_fb_park, NULL);
}

/* LITHE_FOREIGN_BOOTSTRAP=2: the MAIN thread goes through the helper too and stays a plain OS
 * thread. As thread0 it runs on hart 0, and any kernel block it then makes (pthread_join, Python
 * waiting on its worker threads) takes that hart with it: with 2 harts, waves of foreign OpenMP
 * roots joined by main hung 1/200 (2 harts) and 60/60 (1 hart). Only for processes where nothing
 * needs main to be a Lithe context -- hosted MPI does (its ranks are contexts started from main). */
static int fjs_foreign_bootstrap_mode(void)
{
  static int mode = -1;
  if (mode < 0) {
    const char *e = getenv("LITHE_FOREIGN_BOOTSTRAP");
    mode = e ? atoi(e) : 1;
  }
  return mode;
}

static int fjs_foreign_first_caller(void)
{
  int mode = fjs_foreign_bootstrap_mode();
  return mode > 0 && !fjs_fb_is_helper && current_uthread == NULL &&
         __atomic_load_n(&lithe_root_fj_sched, __ATOMIC_ACQUIRE) == NULL &&
         (mode >= 2 || (pid_t)syscall(SYS_gettid) != getpid());
}

static void fjs_foreign_bootstrap(void)
{
  pthread_mutex_lock(&fjs_fb_lock);
  if (fjs_fb_state == 0) {
    pthread_t t;
    pthread_attr_t a;
    pthread_attr_init(&a);
    pthread_attr_setdetachstate(&a, PTHREAD_CREATE_DETACHED);
    if (pthread_create(&t, &a, fjs_fb_helper, NULL) == 0)
      fjs_fb_state = 1;
    pthread_attr_destroy(&a);
  }
  while (fjs_fb_state == 1)
    pthread_cond_wait(&fjs_fb_cond, &fjs_fb_lock);
  pthread_mutex_unlock(&fjs_fb_lock);
}

lithe_fork_join_sched_t *lithe_ensure_root_fork_join_sched(void)
{
  if (fjs_foreign_first_caller()) {
    fjs_foreign_bootstrap();
    lithe_fork_join_sched_t *r = __atomic_load_n(&lithe_root_fj_sched, __ATOMIC_ACQUIRE);
    if (r != NULL)
      return r;
    /* helper could not be created: fall through to the old (hijacking) path */
  }
  /* Base bootstrap (owned by lithe/parlib, idempotent): main -> vcore 0 +
   * parlib vcore pool + base scheduler. Safe to call repeatedly. */
  lithe_ensure_main_on_vcore0();

  /* ADOPT, DO NOT NEST (2026-10-14).
   *
   * If the caller is already inside a fork-join scheduler, THAT scheduler is
   * the root for composition purposes -- adopt it and return. The old code
   * unconditionally created a second FJS and entered it from whatever context
   * called first, which produced two distinct failures:
   *
   *   1. A permanent spurious scheduler layer on every composed run: a
   *      standalone-libomp region inside an app that already had its own FJS
   *      built base -> app FJS -> "root" FJS -> sticky OMP FJS, where the
   *      middle layer exists only because this function insisted on owning a
   *      root it had not been asked for.
   *   2. A guaranteed hang. `lithe_root_fj_entered` is a process-global latch,
   *      so the enter was charged to whichever CONTEXT happened to call first.
   *      When that context is transient (one Lithe context per rank, which is
   *      the whole hosted model) it exits while still inside this scheduler,
   *      leaving a child registered on its parent with granted=1 and harts=0.
   *      base_child_exited() then spins forever waiting for a hart that no one
   *      owns, so the application's own lithe_sched_exit() never returns.
   *
   * Reproduced with ONE OpenMP region of ONE thread inside ONE Lithe context;
   * the region itself computes correctly and teardown hangs. The reproducer and
   * the ablation (no-OMP control exits cleanly) are in
   * .build_logs/ompctx/repro.c.
   *
   * Adopting is also the honest semantic: callers use the result as "the
   * fork-join scheduler to create worker contexts on" (see libomp's
   * __kmp_lithe_worker_sched_for_create), and when you are already inside one,
   * that is the correct answer. */
  if (lithe_root_fj_sched == NULL) {
    lithe_sched_t *cur = lithe_sched_current();
    if (cur != NULL && cur->funcs == &lithe_fork_join_sched_funcs) {
      lithe_fork_join_sched_t *adopted = (lithe_fork_join_sched_t *)cur;
      if (__sync_bool_compare_and_swap(&lithe_root_fj_sched, NULL, adopted)) {
        /* Entered by construction: the caller is running inside it. Nothing to
         * exit later, which is precisely the point. */
        lithe_root_fj_adopted = 1;
        lithe_root_fj_entered = 1;
      }
      if (lithe_root_fj_sched != NULL)
        return lithe_root_fj_sched;
    }
  }

  /* Create the single root FJ scheduler once. */
  if (lithe_root_fj_sched == NULL) {
    lithe_fork_join_sched_t *s = lithe_fork_join_sched_create();
    if (s == NULL)
      return NULL;
    if (!__sync_bool_compare_and_swap(&lithe_root_fj_sched, NULL, s)) {
      /* Lost a first-call race (not expected during normal init): drop ours. */
      lithe_fork_join_sched_destroy(s);
    }
  }

  /* Enter it once, on the main uthread. Mirrors OPAL's ensure path: only enter
   * when there is already a current scheduler (i.e. main is a real uthread on a
   * vcore) and we are not already inside the root. */
  if (!lithe_root_fj_entered) {
    lithe_sched_t *cur = lithe_sched_current();
    if (cur != NULL && cur != (lithe_sched_t *)lithe_root_fj_sched) {
      lithe_sched_enter((lithe_sched_t *)lithe_root_fj_sched);
      lithe_root_fj_entered = 1;
    }
  }
  return lithe_root_fj_sched;
}

int lithe_root_fork_join_sched_entered(void)
{
  return lithe_root_fj_entered;
}

/* 1 when the root fork-join scheduler is the application's own, adopted rather
 * than created by lithe. Callers that would otherwise tear the root down at
 * process exit must not do so in that case. */
int lithe_root_fork_join_sched_adopted(void)
{
  return lithe_root_fj_adopted;
}

/* Number of RUNNABLE-but-unscheduled contexts on the CURRENT fork-join sched,
 * i.e. contexts sitting on this sched's per-vcore runqueues waiting for a hart.
 *
 * A context that is spinning while HOLDING its hart (e.g. a lithified-libomp
 * worker hot-spinning on the team-local inner-barrier flag) must consult this
 * to decide whether it is starving a peer: if any peer is RUNNABLE but has no
 * hart, the spinner has to cooperatively donate its hart (lithe_context_yield)
 * so the peer can run. Otherwise, when the number of OpenMP worker contexts
 * exceeds the available harts/vcores (real oversubscription, e.g. OMP=192 on 96
 * cores, or an under-provisioned VCORE_LIMIT), the hart-holders spin forever on
 * a barrier flag that the never-scheduled peers can never advance -> a userspace
 * hart-starvation livelock (no syscalls, "fewer kernel switches", process hangs
 * and must be reaped). At proper width every worker owns a hart so the count is
 * 0 and the fast hot-spin path is unaffected.
 *
 * Returns 0 when the current sched is not a fork-join sched (e.g. the base
 * sched) or there is no current sched. One relaxed-acquire atomic load; safe to
 * call from any lithe context. */
long lithe_fork_join_current_runnable_count(void)
{
  lithe_sched_t *cur = lithe_sched_current();
  if (cur == NULL || cur->funcs != &lithe_fork_join_sched_funcs)
    return 0;
  return __atomic_load_n(&((lithe_fork_join_sched_t *)cur)->runnable_count,
                         __ATOMIC_ACQUIRE);
}

long lithe_fork_join_current_hartless_runnable(void)
{
  lithe_sched_t *cur = lithe_sched_current();
  lithe_fork_join_sched_t *s;
  long r, owned, running, idle;
  if (cur == NULL || cur->funcs != &lithe_fork_join_sched_funcs)
    return 0;
  s = (lithe_fork_join_sched_t *)cur;
  r = __atomic_load_n(&s->runnable_count, __ATOMIC_ACQUIRE);
  if (r <= 0)
    return 0;
  owned = (long)parlib_atomic_read(&s->sched.harts);
  running = __atomic_load_n(&s->running_count, __ATOMIC_ACQUIRE);
  idle = owned - running;
  if (idle < 0)
    idle = 0;
  r -= idle;
  return r > 0 ? r : 0;
}

static int fjs_parent_starved(lithe_fork_join_sched_t *sched);
static int fjs_wanted_elsewhere = -1;
static inline int fjs_wanted_elsewhere_on(void)
{
  if (fjs_wanted_elsewhere < 0) {
    const char *e = getenv("LITHE_FJS_WANTED_ELSEWHERE");
    fjs_wanted_elsewhere = (e && e[0] == '0') ? 0 : 1; /* default on, B61 */
  }
  return fjs_wanted_elsewhere;
}

int lithe_fork_join_should_yield_to_runnable(void)
{
  long r = lithe_fork_join_current_runnable_count();
  lithe_sched_t *cur = lithe_sched_current();
  lithe_fork_join_sched_t *s;
  long owned, wc, demand, budget;
  int soft_cap;

  /* Self is RUNNING on a hart. Peers on the runqueue need a hart under
   * true scarcity ((r+1) > num_vcores). That alone is wrong for VCORE>>K:
   * with VCORE=96, scarcity never trips, so K ranks + PMIx/OPAL progress
   * helpers each keep a hart and CQ-spin together (P1K2 peak concurrent=4
   * whenever VCORE≥4 → ~4–5× Barrier+Allreduce vs VCORE=2).
   *
   * Effective budget = min(num_vcores, soft_cap) when soft_cap is set from
   * LITHE_FJS_MAX_ONLINE_HARTS or LITHE_CONTEXT_RANKS_PER_HOST. Hart requests
   * are also clamped to that budget (see fjs_clamp_hart_request). Yield when
   * owned >= budget with RUNNABLE peers, or when owned > soft_cap (surplus).
   * Soft_cap+1 spare was removed: at VCORE=4/owned=4 it caused yield churn
   * worse than VCORE=96. When soft_cap unset, classic scarcity only. */
  fjs_tradespace_init_once();
  soft_cap = fjs_soft_hart_cap;
  budget = (long)num_vcores();
  if (soft_cap > 0 && (long)soft_cap < budget)
    budget = (long)soft_cap;
  if (cur != NULL && cur->funcs == &lithe_fork_join_sched_funcs) {
    s = (lithe_fork_join_sched_t *)cur;
    owned = (long)parlib_atomic_read(&s->sched.harts);
    wc = __atomic_load_n(&s->warm_ready_count, __ATOMIC_ACQUIRE);
  } else {
    owned = 0;
    wc = 0;
  }

  if (r > 0) {
    if ((r + 1) > budget)
      return 1;
    /* Cap reached: must multiplex — another hart will not appear. */
    if (owned >= budget)
      return 1;
    /*
     * Helper spare (soft_cap=RPH+HELPER): when owned is already at RPH,
     * do not wait for the spare hart to run a RUNNABLE peer — that stranded
     * pairwise stragglers vs flat Barrier (P1K8/P1K16). Do NOT use
     * owned+r>=budget: multi-OS park+pump leaves a helper RUNNABLE and
     * would suppress CQ park (P2K2 hang).
     */
    if (fjs_helper_harts > 0 && soft_cap > fjs_helper_harts) {
      long rank_budget = (long) soft_cap - (long) fjs_helper_harts;
      if (rank_budget < 1)
        rank_budget = 1;
      if (owned >= rank_budget)
        return 1;
    }
    demand = r + wc + 1;
    if (owned > demand + fjs_surplus_slack)
      return 1;
    return 0;
  }

  /* No runnable peers: still force wait-spinners off surplus harts so
   * progress helpers park/multiplex instead of CQ-contending with ranks. */
  if (soft_cap > 0 && owned > (long)soft_cap)
    return 1;
  /* B61 (LITHE_FJS_WANTED_ELSEWHERE, default 1): "no runnable peer" above only looks at
   * the CURRENT scheduler. Nested libomp keeps each rank master inside its
   * per-team child, so three masters idle-spinning in MPI waits (wait_sync
   * lithe_wsd_idle_spin: "nobody wants this hart") held all three harts while
   * the fourth rank's team sat runnable with granted 0 -- hosted miniFE R=4
   * OMP=2 VCORE_LIMIT=3 hung 5/5. Someone does want the hart if any FJS
   * ancestor on the chain is starved (itself runnable with no harts, or a
   * sibling child that needs one and owns none). */
  if (fjs_wanted_elsewhere_on() && cur != NULL &&
      cur->funcs == &lithe_fork_join_sched_funcs) {
    for (lithe_sched_t *c = cur; c->parent != NULL &&
         c->parent->funcs == &lithe_fork_join_sched_funcs; c = c->parent)
      if (fjs_parent_starved((lithe_fork_join_sched_t *)c))
        return 1;
  }
  return 0;
}

void lithe_fork_join_sched_destroy(lithe_fork_join_sched_t *sched)
{
  lithe_fork_join_sched_cleanup(sched);
  if (wfl_size(&sched_zombie_list) < 100)
    wfl_insert(&sched_zombie_list, sched);
  else
    free(sched);
}

void lithe_fork_join_sched_init(lithe_fork_join_sched_t *sched,
                                lithe_fork_join_context_t *main_context)
{
  for (int i=0; i < max_vcores(); i++) {
    TAILQ_INIT(&tqueue_s(sched, i));
    spin_pdr_init(&tqlock_s(sched, i));
    tqsize_s(sched, i) = 0;
    rseed_s(sched, i) = i;
    vconline_s(sched, i) = false;
    sched->vc_mgmt[i].warm_ctx = NULL;  /* WARM VCORES: empty slot */
  }

  memset(main_context, 0, sizeof(*main_context));
  main_context->state = FJS_CTX_RUNNING;
  main_context->preferred_vcq = vcore_id();
  sched->sched.main_context = &main_context->context;

  sched->num_contexts = 1;
  sched->granting_harts = 0;
  sched->teardown_quiesce = 0;
  sched->runnable_count = 0;  /* CHANGE 1: O(1) any-runnable indicator */
  sched->idle_spinning = 0;
  sched->warm_ready_count = 0;  /* WARM VCORES: GO-and-unclaimed warm workers */
  sched->leaving_harts = 0;  /* LOST-WAKEUP GUARD: committed-to-yield harts */
  sched->running_count = 1;  /* IDLE-AWARE PEER CHECK: the main context is running */
  sched->lpt_ranked_queued = 0;
  sched->lpt_hi_ns = 0;
  sched->lpt_lo_ns = 0;
  TAILQ_INIT(&sched->child_sched_list);
  spin_pdr_init(&sched->child_sched_list_lock);
  /* sched->next_queue_id initialized in sched_enter() */
}

void lithe_fork_join_sched_cleanup(lithe_fork_join_sched_t *sched)
{
}

lithe_fork_join_context_t*
  lithe_fork_join_context_create(lithe_fork_join_sched_t *sched,
                                 size_t stack_size,
                                 void (*start_routine)(void*),
                                 void *arg)
{
  lithe_fork_join_context_t *ctx = __ctx_alloc(sched, stack_size);
  lithe_fork_join_context_init(sched, ctx, start_routine, arg);
  return ctx;
}

void lithe_fork_join_context_init(lithe_fork_join_sched_t *sched,
                                  lithe_fork_join_context_t *ctx,
                                  void (*start_routine)(void*),
                                  void *arg)
{
  ctx->state = FJS_CTX_CREATED;
  ctx->preferred_vcq = -1;
  ctx->start_routine = start_routine;
  ctx->arg = arg;
  ctx->warm_state = FJS_WARM_NONE;  /* WARM VCORES: not parked */
  ctx->warm_go = 0;
  ctx->warm_vcq = -1;
  ctx->warm_sched = NULL;
  ctx->lpt_start_ns = 0;
  ctx->lpt_pred_ns = 0;
  ctx->lpt_polling = 0;
  ctx->daemon = 0;
  ctx->lpt_counted = 0;

  void start_routine_wrapper(void *arg)
  {
    lithe_fork_join_context_t *self = arg;
    self->start_routine(self->arg);
    destroy_dtls();
  }

  lithe_context_init(&ctx->context, start_routine_wrapper, ctx);
  /* lithe_context_init binds TLS current_sched; workers must belong to this FJ sched. */
  lithe_context_reassociate(&ctx->context, &sched->sched);
  __sync_fetch_and_add(&sched->num_contexts, 1);
  schedule_context(ctx, false);
}

void lithe_fork_join_context_cleanup(lithe_fork_join_context_t *context)
{
  lithe_context_cleanup(&context->context);
}

void lithe_fork_join_context_destroy(lithe_fork_join_context_t *context)
{
  lithe_fork_join_context_cleanup(context);
  __ctx_free(context);
}

void lithe_fork_join_sched_join_one(lithe_fork_join_sched_t *sched)
{
  if(__sync_add_and_fetch(&sched->num_contexts, -1) == 0)
    lithe_context_unblock(sched->sched.main_context);
}

void lithe_fork_join_context_set_daemon(lithe_fork_join_context_t *ctx)
{
  if (!ctx || __atomic_exchange_n(&ctx->daemon, 1, __ATOMIC_ACQ_REL))
    return;
  lithe_fork_join_sched_join_one((lithe_fork_join_sched_t *)ctx->context.sched);
}

static void block_main_context(lithe_context_t *c, void *arg)
{
  lithe_fork_join_sched_join_one(arg);
}

void lithe_fork_join_sched_join_all(lithe_fork_join_sched_t *sched)
{
  lithe_context_block(block_main_context, sched);
  /* Take back the main context's own reference (num_contexts starts at 1 for it; the
   * block above dropped it). Without this a second join_all returned when the first
   * context of the round finished, and by the third a context exit unblocked main while
   * it was running: main on two harts at once (2026-10-23, .build_logs/glibc_lithe/joinrep.c:
   * hung 5/5 at one hart, segfaulted 4/5 at four). */
  __sync_fetch_and_add(&sched->num_contexts, 1);
}

void lithe_fork_join_context_migrate(lithe_fork_join_sched_t *from,
                                     lithe_fork_join_sched_t *to,
                                     lithe_fork_join_context_t *ctx)
{
	lithe_context_t *c;
	int i;
	int found;

	if (!from || !to || !ctx || from == to)
		return;
	if (ctx->context.sched != (lithe_sched_t *)from)
		return;

	/* Runnable contexts live on from's per-vcore run queues; remove first. */
	if (ctx->state == FJS_CTX_RUNNABLE) {
		found = 0;
		for (i = 0; i < max_vcores(); i++) {
			spin_pdr_lock(&tqlock_s(from, i));
			TAILQ_FOREACH(c, &tqueue_s(from, i), link) {
				if (c == &ctx->context) {
					TAILQ_REMOVE(&tqueue_s(from, i), c, link);
					fjs_lpt_count_out(from, (lithe_fork_join_context_t *)c);
					tqsize_s(from, i)--;
					/* CHANGE 1: keep the any-runnable indicator in sync when a
					 * RUNNABLE context is pulled off `from`'s queues; the paired
					 * __thread_enqueue_on(to,...) below re-increments `to`. */
					__atomic_fetch_sub(&from->runnable_count, 1, __ATOMIC_SEQ_CST);
					found = 1;
					break;
				}
			}
			spin_pdr_unlock(&tqlock_s(from, i));
			if (found)
				break;
		}
		assert(found && "migrate: RUNNABLE context not on from's queues");
	}

	if (!__atomic_load_n(&ctx->daemon, __ATOMIC_ACQUIRE))
		__sync_fetch_and_add(&from->num_contexts, -1);
	lithe_context_reassociate(&ctx->context, &to->sched);
	if (!__atomic_load_n(&ctx->daemon, __ATOMIC_ACQUIRE))
		__sync_fetch_and_add(&to->num_contexts, 1);

	if (ctx->state == FJS_CTX_RUNNABLE) {
		__thread_enqueue_on(to, ctx, false);
		/* lithe_hart_request() uses lithe_sched_current() as the child; caller
		 * must request harts after lithe_sched_enter(to) when migrating from
		 * outside `to`. */
	}
}

void lithe_fork_join_sched_hart_request(lithe_sched_t *__this,
                                       lithe_sched_t *child,
                                       int h)
{
  uint16_t *harts_needed = (uint16_t*)&child->parent_data + 1;
  __sync_fetch_and_add(harts_needed, h);
  lithe_hart_request(h);
}

void lithe_fork_join_sched_sched_enter(lithe_sched_t *__this)
{
	lithe_fork_join_sched_t *sched = (void *)__this;
	vconline(vcore_id()) = true;
	sched->next_queue_id = vcore_id();
	/* Re-entry after an exit (libomp's sticky FJ is re-entered at teardown). */
	sched->teardown_quiesce = 0;
}

void lithe_fork_join_sched_sched_exit(lithe_sched_t *__this)
{
	lithe_fork_join_sched_t *sched = (void *)__this;
	uint16_t *harts_needed;

	vconline(vcore_id()) = false;
	/*
	 * Stop demanding harts from the parent. Otherwise idle grants can keep
	 * child->harts > 0 forever and lithe_sched_exit()'s drain loop never
	 * finishes (hosted RPH>1: MPI_Finalize OK, hang in sched_exit).
	 */
	harts_needed = (uint16_t *)&sched->sched.parent_data + 1;
	*harts_needed = 0;
	/* EXIT QUIESCE: tell this sched's other harts to leave now instead of
	 * idle-parking in hart_enter (see lithe.c base_child_exited). The field
	 * existed but nothing ever set or read it. Poke parked harts. */
	if (lithe_exit_quiesce_on()) {
		__atomic_store_n(&sched->teardown_quiesce, 1, __ATOMIC_SEQ_CST);
		parlib_reactor_wake();
	}
}

void lithe_fork_join_sched_child_enter(lithe_sched_t *__this,
                                       lithe_sched_t *child)
{
	lithe_fork_join_sched_t *sched = (void *)__this;
	uint16_t *harts_granted = (uint16_t*)&child->parent_data;
	uint16_t *harts_needed = (uint16_t*)&child->parent_data + 1;
	*harts_granted = 1;
	*harts_needed = 1;

	spin_pdr_lock(&sched->child_sched_list_lock);
	TAILQ_INSERT_TAIL(&sched->child_sched_list, child, link);
	spin_pdr_unlock(&sched->child_sched_list_lock);

	if (lithe_sched_trace_enabled())
		lithe_sched_trace_emit(LITHE_ST_CHILD_ENTER, sched, 0, LITHE_ST_DEC_OK,
		                       (int32_t)(uintptr_t)child);
}

void lithe_fork_join_sched_child_exit(lithe_sched_t *__this,
                                      lithe_sched_t *child)
{
  lithe_fork_join_sched_t *sched = (void *)__this;
  int spins = 0;

  spin_pdr_lock(&sched->child_sched_list_lock);
  TAILQ_REMOVE(&sched->child_sched_list, child, link);
  spin_pdr_unlock(&sched->child_sched_list_lock);

  if (lithe_sched_trace_enabled())
	  lithe_sched_trace_emit(LITHE_ST_CHILD_EXIT, sched, 0, LITHE_ST_DEC_OK,
	                         (int32_t)(uintptr_t)child);

  /* Bound the wait: a stuck in-flight grant must not freeze sched_exit
   * (hosted RPH>1 saw MPI_Finalize OK then hang here). */
  while (sched->granting_harts && spins++ < 10000000)
    cpu_relax();
}


void lithe_fork_join_sched_hart_return(lithe_sched_t *__this,
                                       lithe_sched_t *child)
{
	uint16_t *harts_granted = (uint16_t*)&child->parent_data;
	atomic_add(harts_granted, -1);
	vconline(vcore_id()) = true;
	if (lithe_sched_trace_enabled())
		lithe_sched_trace_emit(LITHE_ST_HART_RETURN, __this, 0, LITHE_ST_DEC_OK,
		                       (int32_t)(uintptr_t)child);
}

static void decrement(void *gh)
{
  __sync_fetch_and_add((size_t*)gh, -1);
}

/* Hint: child FJS runnable+warm, or -1 if child is not FJS. */
static int fjs_child_work_hint(lithe_sched_t *child)
{
	if (!child || child->funcs != &lithe_fork_join_sched_funcs)
		return -1;
	lithe_fork_join_sched_t *cs = (lithe_fork_join_sched_t *)child;
	long r = __atomic_load_n(&cs->runnable_count, __ATOMIC_RELAXED);
	long w = __atomic_load_n(&cs->warm_ready_count, __ATOMIC_RELAXED);
	return (int)(r + w);
}

/* True if child has outstanding hart demand (harts_needed > harts_granted).
 * Ledger-only on purpose: this runs in the hot hart_enter spin/park loops;
 * adding a work-hint probe here made every transient enqueue (yield
 * re-enqueue that the yielding hart itself re-dequeues) restart idle harts
 * and stampede grants (~2x P2K2 nx=80 CG). Drift healing lives in
 * fjs_heal_grant_starved_child, called only at the hart's final exit. */
static int fjs_child_needs_hart(lithe_sched_t *child)
{
	uint16_t *harts_granted = (uint16_t *)&child->parent_data;
	uint16_t *harts_needed = (uint16_t *)&child->parent_data + 1;
	return *harts_granted < *harts_needed;
}

/* SELF-HEAL (last-exit only): a fork-join child whose ledger says satisfied
 * (needed <= granted) but which has queued RUNNABLE/warm work below the soft
 * hart cap. With a drifted ledger nothing ever grants again and the child's
 * busy harts may never yield (single-OS wait_sync spin policy) — permanent
 * stall (hosted miniFE P1K4 nx=80, live gdb: needed=1 < granted=2,
 * runnable=1). Called only when a hart is about to lithe_hart_yield() away
 * from the parent, so the probe costs nothing in steady state but the last
 * departing hart always rescues stranded child work. */
static int fjs_child_ledger_starved(lithe_sched_t *child)
{
	uint16_t *harts_granted = (uint16_t *)&child->parent_data;
	uint16_t *harts_needed = (uint16_t *)&child->parent_data + 1;
	if (*harts_granted < *harts_needed)
		return 0; /* normal grant path will serve it */
	if (fjs_child_work_hint(child) <= 0)
		return 0;
	{
		long owned = (long)parlib_atomic_read(
			&((lithe_fork_join_sched_t *)child)->sched.harts);
		if (fjs_soft_hart_cap > 0 && owned >= (long)fjs_soft_hart_cap)
			return 0;
	}
	return 1;
}

static int fjs_any_child_ledger_starved(lithe_fork_join_sched_t *sched)
{
	int starved = 0;
	lithe_sched_t *child;

	if (TAILQ_EMPTY(&sched->child_sched_list))
		return 0;
	spin_pdr_lock(&sched->child_sched_list_lock);
	TAILQ_FOREACH(child, &sched->child_sched_list, link) {
		if (fjs_child_ledger_starved(child)) {
			starved = 1;
			break;
		}
	}
	spin_pdr_unlock(&sched->child_sched_list_lock);
	return starved;
}

/* Lift each starved child's needed to granted+1 so the normal grant path
 * re-opens; caller then loops back through hart_enter (grant_children). */
static void fjs_heal_starved_children(lithe_fork_join_sched_t *sched)
{
	lithe_sched_t *child;

	spin_pdr_lock(&sched->child_sched_list_lock);
	TAILQ_FOREACH(child, &sched->child_sched_list, link) {
		if (!fjs_child_ledger_starved(child))
			continue;
		{
			uint16_t *harts_granted = (uint16_t *)&child->parent_data;
			uint16_t *harts_needed = (uint16_t *)&child->parent_data + 1;
			int16_t delta =
				(int16_t)(*harts_granted - *harts_needed) + 1;
			if (delta > 0)
				__sync_fetch_and_add(harts_needed,
				                     (uint16_t)delta);
		}
	}
	spin_pdr_unlock(&sched->child_sched_list_lock);
}

/* Peek whether any registered child still wants a hart. Used to avoid
 * idle-spin → hart_enter restart thrash when the child list is non-empty
 * but nobody needs a grant (profile: fjs_try_hart_grant_child / spinlock). */
static int fjs_any_child_needs_hart(lithe_fork_join_sched_t *sched)
{
	int need = 0;
	lithe_sched_t *child;

	if (TAILQ_EMPTY(&sched->child_sched_list))
		return 0;
	spin_pdr_lock(&sched->child_sched_list_lock);
	TAILQ_FOREACH(child, &sched->child_sched_list, link) {
		if (fjs_child_needs_hart(child)) {
			need = 1;
			break;
		}
	}
	spin_pdr_unlock(&sched->child_sched_list_lock);
	return need;
}

/* Try to grant one hart to child; may not return if grant succeeds. */
static void fjs_try_hart_grant_child(lithe_fork_join_sched_t *sched,
                                     lithe_sched_t *child)
{
  uint16_t *harts_granted = (uint16_t*)&child->parent_data;
  uint16_t *harts_needed = (uint16_t*)&child->parent_data + 1;
  /* Cheap reject before atomic_add: empty demand still paid spinlock tax.
   * (Drift healing is NOT done here: this runs on every grant rotation and
   * an inline work-hint heal stampeded extra grants on transient enqueues.
   * See fjs_heal_starved_children, driven from the hart's final exit.) */
  if (*harts_granted >= *harts_needed)
    return;
  if (atomic_add(harts_granted, 1) + 1 <= *harts_needed) {
    if (lithe_sched_trace_enabled()) {
      uint8_t dec = LITHE_ST_DEC_OK;
      long parent_r =
          __atomic_load_n(&sched->runnable_count, __ATOMIC_RELAXED);
      int child_w = fjs_child_work_hint(child);
      if (parent_r > 0)
        dec |= LITHE_ST_DEC_GRANT_PARENT_RUNNABLE;
      if (child_w == 0)
        dec |= LITHE_ST_DEC_GRANT_EMPTY_CHILD;
      lithe_sched_trace_emit(LITHE_ST_HART_GRANT, sched, 0, dec,
                             child_w >= 0 ? child_w : (int32_t)*harts_needed);
    }
    vconline(vcore_id()) = false;
    lithe_hart_grant(child, decrement, &sched->granting_harts);
  }
  atomic_add(harts_granted, -1);
}

static int fjs_idle_trim = -1;
static inline int fjs_idle_trim_on(void)
{
  if (__builtin_expect(fjs_idle_trim < 0, 0)) {
    const char *e = getenv("LITHE_FJS_IDLE_TRIM");
    fjs_idle_trim = (e && e[0] == '0') ? 0 : 1;
  }
  return fjs_idle_trim;
}

static int fjs_child_idle_leave = -1;
static inline int fjs_child_idle_leave_on(void)
{
  if (__builtin_expect(fjs_child_idle_leave < 0, 0)) {
    const char *e = getenv("LITHE_FJS_CHILD_IDLE_LEAVE");
    fjs_child_idle_leave = (e && e[0] == '0') ? 0 : 1; /* default on, B61 */
  }
  return fjs_child_idle_leave;
}

static void fjs_hart_grant_children(lithe_fork_join_sched_t *sched)
{
  if (TAILQ_EMPTY(&sched->child_sched_list))
    return;
  /* No child with harts_needed > harts_granted: skip list rotate + atomics. */
  if (!fjs_any_child_needs_hart(sched))
    return;

  fjs_tradespace_init_once();
  __sync_fetch_and_add(&sched->granting_harts, 1);

  /* progress_first: dedicated pass for the progress child sched before rotation. */
  if (fjs_hart_order == LITHE_FJS_HART_ORDER_PROGRESS_FIRST &&
      lithe_progress_child_sched != NULL) {
    fjs_try_hart_grant_child(sched, (lithe_sched_t *)lithe_progress_child_sched);
  }

  lithe_sched_t *first = NULL;
  while (1) {
    spin_pdr_lock(&sched->child_sched_list_lock);
    lithe_sched_t *child = TAILQ_FIRST(&sched->child_sched_list);
    if (child) {
      TAILQ_REMOVE(&sched->child_sched_list, child, link);
      TAILQ_INSERT_TAIL(&sched->child_sched_list, child, link);
    }
    spin_pdr_unlock(&sched->child_sched_list_lock);
    if (!child)
      break;

    if (fjs_hart_order == LITHE_FJS_HART_ORDER_PROGRESS_FIRST &&
        child == (lithe_sched_t *)lithe_progress_child_sched)
      continue;

    fjs_try_hart_grant_child(sched, child);

    if (first == NULL)
      first = child;
    else if (first == child)
      break;
  }
  decrement(&sched->granting_harts);
}

static void fjs_hart_run_warm_or_dequeue(lithe_fork_join_sched_t *sched)
{
  if (__builtin_expect(fjs_l3_confine < 0, 0))
    fjs_l3_confine_init();
  if (fjs_l3_confine) {
    int me = vcore_id();
    if (me >= 0 && me < FJS_TOPO_MAX_VC && fjs_vc_home[me] == 0) {
      int mv = (int)max_vcores(), idle_home = 0;
      for (int v = 0; v < mv && v < FJS_TOPO_MAX_VC; v++)
        if (fjs_vc_home[v] > 0 && vconline_s(sched, v) &&
            !__atomic_load_n(&fjs_vc_busy[v], __ATOMIC_ACQUIRE)) {
          idle_home = 1;
          break;
        }
      if (idle_home) {
        if (__atomic_load_n(&sched->runnable_count, __ATOMIC_ACQUIRE) > 0)
          parlib_reactor_wake();
        return;
      }
    }
  }
  if (__atomic_load_n(&sched->warm_ready_count, __ATOMIC_ACQUIRE) > 0) {
    lithe_fork_join_context_t *w = fjs_warm_claim(sched, vcore_id());
    if (w != NULL) {
      assert(w->context.uth.tls_desc != NULL);
      w->state = FJS_CTX_RUNNING;
      fjs_lpt_start(w);
      if (lithe_sched_trace_enabled())
        lithe_sched_trace_emit(LITHE_ST_WARM_CLAIM, sched,
                               (uint32_t)w->context.id, LITHE_ST_DEC_OK, 0);
      fjs_stats_run_enter();
      __atomic_fetch_add(&sched->running_count, 1, __ATOMIC_SEQ_CST);
      fjs_vc_set_busy(1);
      lithe_context_run(&w->context);
    }
  }

  lithe_fork_join_context_t *ctx = __thread_dequeue();
  if (ctx != NULL) {
    if (ctx->start_routine == FJS_CTX_FREED_POISON) {
      fprintf(stderr, "[LITHE FJS] FATAL: dequeued freed context %p "
              "(id=%d state=%d sched=%p) -- use-after-free: a destroyed "
              "worker context was still on the runqueue.\n",
              ctx, ctx->context.id, ctx->state, sched);
      abort();
    }
    if (ctx->context.uth.tls_desc == NULL) {
      fprintf(stderr, "[LITHE FJS] FATAL: dequeued context %p (id=%d "
              "state=%d sched=%p) has tls_desc==NULL -- its TLS was freed "
              "while it was still runnable.\n",
              ctx, ctx->context.id, ctx->state, sched);
      abort();
    }
    assert(ctx->state == FJS_CTX_RUNNABLE);
    ctx->state = FJS_CTX_RUNNING;
    fjs_lpt_start(ctx);
    fjs_stats_run_enter();
    __atomic_fetch_add(&sched->running_count, 1, __ATOMIC_SEQ_CST);
      fjs_vc_set_busy(1);
    lithe_context_run(&ctx->context);
  }
}

/* PARENT-STARVATION YIELD (2026-10-15, env LITHE_FJS_PARENT_STARVE_YIELD=1,
 * default off for A/B).
 *
 * A Lithe child never returns a hart to its parent while it has runnable work,
 * and a context that spin-waits with lithe_context_yield() is ALWAYS runnable
 * (context_yield re-enqueues it and the same hart re-enters). So when every
 * hart is inside children whose contexts are spin-waiting on something only a
 * context of the PARENT can release, nothing ever runs the parent.
 *
 * Measured on the login node with .build_logs/ompctx/repro: 3 Lithe contexts
 * each opening a team of 2 on VCORE_LIMIT=3 hangs 3/3. gdb: app sched harts=0
 * runnable=1; its two OMP child scheds hold all 3 harts; libomp's
 * __kmp_forkjoin_lock (a TICKET lock) has now_serving=5 next_ticket=8 and was
 * last RELEASED -- the context holding ticket 5 is the parent's runnable one,
 * and tickets 6,7 spin-yield inside the children. Same hang stops hosted
 * miniFE OMP_NUM_THREADS=2 at its first `omp parallel for`.
 *
 * Rule: a child hart that finds its FJS parent with runnable work and NO harts
 * gives the hart back; the parent then runs its own work before re-granting
 * (fjs_self_first_now) or the hart would ping-pong straight back. */

/* Per-vcore count of consecutive context_yield re-entries (spin-wait streak).
 * The starvation hand-off fires only after a full slice: handing off on the
 * FIRST re-entry made every freshly granted hart bounce straight back to the
 * parent when all siblings were starved (gdb: 4 OMP children runnable=1-2,
 * harts=0, both rank-sched harts looping grant->yield->grant; miniFE never
 * printed). LITHE_FJS_STARVE_YIELD_SLICE (default 32). */
#define FJS_MAX_SLICE_VC 1024
static int fjs_yield_streak[FJS_MAX_SLICE_VC];
static int fjs_starve_slice = 32;
static int fjs_parent_starve_yield = -1;
static int fjs_parent_starve_yield_on(void)
{
  if (fjs_parent_starve_yield < 0) {
    const char *e = getenv("LITHE_FJS_PARENT_STARVE_YIELD");
    fjs_parent_starve_yield = (e && e[0] == '0') ? 0 : 1; /* default on, B61 */
    const char *sl = getenv("LITHE_FJS_STARVE_YIELD_SLICE");
    if (sl && atoi(sl) > 0)
      fjs_starve_slice = atoi(sl);
  }
  return fjs_parent_starve_yield;
}
static int fjs_parent_starved(lithe_fork_join_sched_t *sched)
{
  lithe_sched_t *p = sched->sched.parent;
  if (p == NULL || p->funcs != &lithe_fork_join_sched_funcs)
    return 0;
  lithe_fork_join_sched_t *pf = (lithe_fork_join_sched_t *)p;
  if ((long)parlib_atomic_read(&p->harts) == 0 &&
      __atomic_load_n(&pf->runnable_count, __ATOMIC_ACQUIRE) > 0)
    return 1;
  /* SIBLING starvation (hosted miniFE RPH=4 x OMP=2, VCORE_LIMIT=3): the
   * parent is the rank FJS with nothing runnable itself, but two of its four
   * per-rank OMP children hold every hart (masters spinning in MPI waits /
   * hosted sync) while the other two have RUNNABLE work and zero harts. Hand
   * the hart to the parent; its grant rotation passes it to the starved one. */
  int starved = 0;
  lithe_sched_t *sib;
  spin_pdr_lock(&pf->child_sched_list_lock);
  TAILQ_FOREACH(sib, &pf->child_sched_list, link) {
    if (sib == &sched->sched)
      continue;
    if ((long)parlib_atomic_read(&sib->harts) == 0 &&
        (fjs_child_needs_hart(sib) || fjs_child_work_hint(sib) > 0)) {
      starved = 1;
      break;
    }
  }
  spin_pdr_unlock(&pf->child_sched_list_lock);
  return starved;
}
/* Unmet demand at some FJS edge from sched up to the root: an ancestor with
 * runnable contexts of its own, or a sibling (on the way up) whose ledger has
 * needed > granted. Ledger-only, like fjs_child_needs_hart. */
static int fjs_chain_demand(lithe_fork_join_sched_t *sched)
{
  for (lithe_sched_t *c = &sched->sched; c->parent != NULL &&
       c->parent->funcs == &lithe_fork_join_sched_funcs; c = c->parent) {
    lithe_fork_join_sched_t *pf = (lithe_fork_join_sched_t *)c->parent;
    if (__atomic_load_n(&pf->runnable_count, __ATOMIC_ACQUIRE) > 0)
      return 1;
    int want = 0;
    lithe_sched_t *sib;
    spin_pdr_lock(&pf->child_sched_list_lock);
    TAILQ_FOREACH(sib, &pf->child_sched_list, link)
      if (sib != c && fjs_child_needs_hart(sib)) {
        want = 1;
        break;
      }
    spin_pdr_unlock(&pf->child_sched_list_lock);
    if (want)
      return 1;
  }
  return 0;
}
/* Exported for runtimes that keep a hart while idle (libomp's fork/join
 * barrier, wait_sync): 1 when the current FJS scheduler's chain to the root
 * has unmet demand elsewhere, so the waiter should block and let the hart go.
 * Takes each ancestor's child-list lock; callers rate-limit it. */
int lithe_fork_join_hart_wanted_elsewhere(void)
{
  lithe_sched_t *cur = lithe_sched_current();
  if (cur == NULL || cur->funcs != &lithe_fork_join_sched_funcs)
    return 0;
  return fjs_chain_demand((lithe_fork_join_sched_t *)cur);
}
/* Own work first when this hart is the only one this sched owns and its own
 * contexts are waiting: otherwise a hart handed back by a child is granted
 * straight back to that child before the parent's context ever runs. */
static int fjs_self_first_now(lithe_fork_join_sched_t *sched)
{
  return fjs_parent_starve_yield_on() &&
         (long)parlib_atomic_read(&sched->sched.harts) <= 1 &&
         __atomic_load_n(&sched->runnable_count, __ATOMIC_ACQUIRE) > 0;
}

void lithe_fork_join_sched_hart_enter(lithe_sched_t *__this)
{
  lithe_fork_join_sched_t *sched = (void *)__this;
  int idle_yield_released_demand = 0;
  /* GAPS E9: set on the progress-child idle-leave path. Reactor waiters are not this child's
   * work (pending is process-global): counting them in the lost-wakeup re-check below sent a
   * leaving hart back to restart forever, and the child kept all 3 harts (granted=3,
   * needed=0, nothing runnable; LITHE_PROGRESS_SCHED=child + LITHE_SC_DMA=1, nbc, 2/20). */
  int prog_child_leave = 0;
  int spin_counted = 0; /* LITHE_FJS_WAKE_LOCAL: registered in idle_spinning */
  if (!vconline(vcore_id()))
    vconline(vcore_id()) = true;

  /* LOST-WAKEUP GUARD: consume one leaving-hart credit (bounded at 0). Each
   * idle yield adds exactly one credit before its final runnable re-check;
   * each hart entering here consumes at most one, so credits track harts
   * whose sched.harts decrement may still be in flight. */
  {
    long lv = __atomic_load_n(&sched->leaving_harts, __ATOMIC_SEQ_CST);
    while (lv > 0 &&
           !__atomic_compare_exchange_n(&sched->leaving_harts, &lv, lv - 1,
                                        false, __ATOMIC_SEQ_CST,
                                        __ATOMIC_SEQ_CST))
      ;
  }

  if (lithe_sched_trace_enabled()) {
    lithe_sched_trace_emit(LITHE_ST_HART_ENTER, sched, 0, LITHE_ST_DEC_OK,
                           (int32_t)__atomic_load_n(&sched->runnable_count,
                                                     __ATOMIC_RELAXED));
  }

restart:
  prog_child_leave = 0;
  if (spin_counted) {
    (void)fjs_spinner_leave(sched); /* restarting serves any claimed wake */
    spin_counted = 0;
  }
  /* Scheduler-driven netpoll: drain completed fd waits before selecting the
   * next continuation. This keeps I/O readiness in the normal Lithe handoff
   * path instead of requiring a helper pthread. */
  fjs_reactor_poll();

  fjs_tradespace_init_once();

  if (__atomic_load_n(&sched->teardown_quiesce, __ATOMIC_ACQUIRE) &&
      __atomic_load_n(&sched->runnable_count, __ATOMIC_SEQ_CST) <= 0 &&
      __atomic_load_n(&sched->warm_ready_count, __ATOMIC_SEQ_CST) <= 0 &&
      (fjs_quiesce_ignore_pending_on() || parlib_reactor_pending() <= 0)) {
    /* Exiting sched with nothing left to run: leave immediately.
     * B20 (2026-10-16): parlib_reactor_pending() is PROCESS-global, not
     * per-sched. The hosted RPH>1 harness calls lithe_sched_exit() on its
     * rank sched before MPI_Finalize(), so the PMIx progress context (which
     * lives in OPAL's sched, not ours) is still parked on its event-base fd
     * and pending stays 1 for the rest of the process. With the pending
     * condition in this gate, every hart of the exiting sched stayed in the
     * `while (pending > 0) drive(-1)` loop below forever, child->harts never
     * reached 0, and lithe_sched_exit() burned its 2,000,000-yield drain
     * bound (each yield = reactor poll + wake_efd write = 30-50 us of
     * kernel time): 60-100 s exits, >90 s timeouts in the stress harness,
     * and the 6-15 s "teardown is not reliably fast" of GAPS B14 at smaller
     * spin cost. Reactor waiters are never stranded by our leaving: the
     * parent's harts drive the reactor, and a woken context re-requests a
     * hart through schedule_context -> lithe_hart_request. */
    vconline(vcore_id()) = false;
    lithe_hart_yield();
  }

  /* B42 kernel-block compensation: once the blocked vcore's thread is back, more
   * vcores run than VCORE_LIMIT admits; shed the surplus here (harts pass through
   * hart_enter on every switch; a lithified hart rarely reaches the base sched). */
  if (parlib_vcores_over_cap()) {
    vconline(vcore_id()) = false;
    lithe_hart_yield();
  }

  if (fjs_parent_starve_yield_on()) {
    int vc = vcore_id();
    if (vc < FJS_MAX_SLICE_VC && fjs_yield_streak[vc] >= fjs_starve_slice &&
        fjs_parent_starved(sched)) {
      fjs_yield_streak[vc] = 0;
      /* Keep our needed ledger: our runnable work still wants this hart back. */
      vconline(vc) = false;
      lithe_hart_yield();
    }
  }

  {
    /* Default and progress_first: children before own work; self_first inverts. */
    int self_first = fjs_hart_order == LITHE_FJS_HART_ORDER_SELF_FIRST ||
                     fjs_self_first_now(sched);
    if (!self_first)
      fjs_hart_grant_children(sched);

    fjs_hart_run_warm_or_dequeue(sched);

    if (self_first)
      fjs_hart_grant_children(sched);
  }

  /* No runnable contexts. If any continuations are parked on fd readiness,
   * this scheduler handoff blocks the vcore in epoll_wait until an fd fires
   * or the reactor wake fd is poked by a newly-runnable context, then loops
   * back through the full scheduler decision path. */
  /* LITHE_FJS_REACTOR_DRIVERS (2026-10-17, GAPS C12): how many idle harts may
   * sit in the infinite reactor drive at once. Default 1; 0 = unlimited (old).
   * With the PMIx progress context parked on its event fd for the whole
   * process, pending stays > 0 and EVERY idle hart used to block here in
   * epoll_wait on the one shared epoll fd. A wake poke (one eventfd write)
   * wakes at most one such sleeper, and the busy harts' zero-timeout polls on
   * hart re-enter drain the eventfd first, so the sleeper goes back to sleep
   * inside epoll_wait. XSBench NP=2 OMP=4: two workers released at +0.4 ms
   * into a 250 ms region were run only when the region ended, because the
   * two idle harts never woke (traced, .build_logs/perfetto/gaps_20261017).
   * Harts beyond the driver budget fall through to the adaptive spin, the
   * timed park and the surplus yield to parlib, where vcore_request() wakes
   * exactly the vcores asked for through their own futex. */
  fjs_tradespace_init_once();
  /* GAPS E9 (2026-10-25): the LITHE_PROGRESS_SCHED=child progress scheduler must not wait.
   * parlib_reactor_pending() is process-global, so its idle hart took the driver slot and
   * parked in the reactor for the whole run while the child's ledger said granted=1: the
   * rank sched lost one of its 3 harts (p2p 8 B, 4 ctx: 56 -> ~105 us/op). Only the bottom
   * drives the reactor; a woken progress context re-requests a hart for this child through
   * schedule_context. So an idle progress-child hart releases its demand down to the harts
   * the child will still hold, and leaves through the lost-wakeup protocol below.
   * LITHE_PROGRESS_CHILD_DRIVE=1 restores the old spin/park/drive path. */
  /* GAPS B61 (2026-10-25) LITHE_FJS_CHILD_IDLE_LEAVE (default 1; =0 disables): the same rule for
   * EVERY child fork-join scheduler. Hosted miniFE, 4 ranks x 2 libomp threads in nested mode
   * (a team scheduler per rank under the rank scheduler), 3 harts: one team scheduler held 2
   * harts with 1 context running -- its idle hart parked in the reactor driver slot -- while two
   * sibling teams sat runnable with granted=0 (gdb ledger, .build_logs/b60/mfe_hang). Hung 5/5.
   * An idle child hart now sheds its surplus demand and returns to its parent at once: no spin,
   * no park, no reactor drive; only the bottom scheduler waits.
   * Only when some FJS ancestor or a sibling on the way up has unmet demand (fjs_chain_demand):
   * gating on fjs_parent_starved up the chain (owns NO harts) instead hung nested 1/10 -- a sibling holding
   * one hart but needing two, or a parent whose harts are all lent to children, was not
   * "starved" -- and slowed it (CG 0.487 -> 0.549 s). Leaving
   * unconditionally made every flat rank scheduler (child of the root) drop its hart on each
   * idle moment and re-request it, hosted ranks K=8 mean 536 -> 563 us, p99 831 -> 1043 us,
   * flat miniFE+OMP CG +1.7 % (n=8, .build_logs/b60/b61_gate.txt). */
  if ((fjs_progress_sched_child && (void *)sched == (void *)lithe_progress_child_sched &&
       !fjs_progress_child_drive()) ||
      (fjs_child_idle_leave_on() && sched->sched.parent &&
       sched->sched.parent->funcs == &lithe_fork_join_sched_funcs &&
       !fjs_any_child_needs_hart(sched) && fjs_chain_demand(sched))) {
    uint16_t *pg = (uint16_t *)&sched->sched.parent_data;
    uint16_t *pn = (uint16_t *)&sched->sched.parent_data + 1;
    int surplus = (int)*pn - ((int)*pg - 1);
    if (surplus > 0)
      lithe_hart_request(-surplus);
    prog_child_leave = 1;
    goto fjs_idle_leave;
  }
  /* LITHE_FJS_SPIN_BEFORE_DRIVE (default ON since 2026-10-22, =0 disables): in a lithified MPI process pending is
   * never 0, so the driver-slot hart blocked in epoll_wait on every idle moment and each
   * schedule_context woke it with an eventfd write: LULESH 1 rank x 8 threads on 4 harts made
   * ~60k write + ~20k epoll_wait + ~20k read per second, ~34 % of samples in the kernel, 1830 vs
   * 2700 zones/s for the same libomp without lithified MPI (which never parks). Spin for runnable
   * WORK first (the adaptive budget below; pending>0 alone is not work) and drive only if none
   * appears. Socket readiness is still served: the driver after the spin, busy harts' timed polls.
   * A/B (GAPS C20): lithified LULESH over 1792-1866 -> 2618-2735 zones/s (= Lithe libomp preload),
   * GROMACS over 127-141 -> 159-161 ns/day; balanced cases, XSBench, LAMMPS, CoMD, miniAMR, hosted
   * miniFE CG unchanged; hosted ranks K=8 8.69 vs 8.84 ms/iter median (9 pairs, within noise). */
  if (fjs_spin_before_drive_on() && fjs_pending_is_work()) {
    fjs_hart_enter_spin_init();
    int my_vc = vcore_id();
    unsigned int budget = fjs_hart_enter_spin_loops > 0 ? fjs_hart_enter_spin_budget(my_vc) : 0;
    for (unsigned int i = 0; i < budget; i++) {
      cpu_relax();
      if (tqsize_s(sched, my_vc) > 0 ||
          __atomic_load_n(&sched->runnable_count, __ATOMIC_ACQUIRE) > 0 ||
          __atomic_load_n(&sched->warm_ready_count, __ATOMIC_ACQUIRE) > 0 ||
          ((i & 1023u) == 0u && fjs_any_child_needs_hart(sched))) {
        fjs_hart_enter_spin_update(my_vc, /*succeeded=*/1);
        goto restart;
      }
    }
    if (budget)
      fjs_hart_enter_spin_update(my_vc, /*succeeded=*/0);
    if (spin_counted) {
      spin_counted = 0;
      if (fjs_spinner_leave(sched) ||
          __atomic_load_n(&sched->runnable_count, __ATOMIC_SEQ_CST) > 0)
        goto restart; /* a waker counted on this hart */
    }
  }
  while (parlib_reactor_pending() > 0) {
    if (fjs_reactor_drivers_max > 0) {
      long d = __atomic_load_n(&fjs_reactor_drivers, __ATOMIC_ACQUIRE);
      if (d >= fjs_reactor_drivers_max)
        break;
      if (!__atomic_compare_exchange_n(&fjs_reactor_drivers, &d, d + 1, false,
                                       __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE))
        continue;
      if (lithe_sched_trace_enabled())
        lithe_sched_trace_emit(LITHE_ST_REACTOR_BLOCK, sched, 0, LITHE_ST_DEC_OK,
                               (int32_t)parlib_reactor_pending());
      (void)fjs_reactor_drive_parked(sched, vcore_id(), -1);
      __atomic_fetch_sub(&fjs_reactor_drivers, 1, __ATOMIC_ACQ_REL);
      fjs_driver_handoff(sched);
      goto restart;
    }
    if (lithe_sched_trace_enabled())
      lithe_sched_trace_emit(LITHE_ST_REACTOR_BLOCK, sched, 0, LITHE_ST_DEC_OK,
                             (int32_t)parlib_reactor_pending());
    /* timeout_ms < 0: must drain waiters; skip empty-runqueue recheck. */
    (void)fjs_reactor_drive_parked(sched, vcore_id(), -1);
    goto restart;
  }

  /* Adaptive spin-then-park: a bounded cpu_relax loop catches work that
   * becomes runnable in the few hundred ns after we drained the runqueue.
   * The check cost is two integer-load checks per pause, so a busy run loop
   * pays at most fjs_hart_enter_spin_cur[vc] PAUSE-ish cycles before
   * parking. Per-vcore counter doubles on success (work appeared during
   * the spin) and halves on failure (had to park); bounded by
   * LITHE_FJS_HART_ENTER_SPIN_{MIN,MAX}. LITHE_FJS_HART_ENTER_SPIN_LOOPS=0
   * disables. */
  fjs_hart_enter_spin_init();
  if (fjs_hart_enter_spin_loops > 0 && !fjs_idle_direct(sched)) {
    int my_vc = vcore_id();
    unsigned int budget = fjs_hart_enter_spin_budget(my_vc);
    /* NOTE: clamping budget to spin_min when empty+soft_cap was tried with
     * wake-on-yield and still regressed P1K2 (timeouts / ~0.5–1ms). Keep
     * full adaptive spin before timed park. */
    unsigned int home_wait = 0;
    if (fjs_wake_local_on()) {
      __atomic_fetch_add(&sched->idle_spinning, 1, __ATOMIC_SEQ_CST);
      spin_counted = 1;
    }
    for (unsigned int i = 0; i < budget; i++) {
      cpu_relax();
      /* Hot path: only cheap atomics every iter. parlib_reactor_pending() and
       * child-list peeks go through PLT/spinlock — every-iter pending was
       * ~32% of hosted P1K4 cycles. Sample those every 64th pause. */
      if ((i & 63u) == 0u) {
        /* LITHE_FJS_IDLE_PENDING_NEEDS_SLOT (2026-10-22): a pending reactor
         * waiter is work for this hart only if it can take a driver slot.
         * Otherwise a hart that lost the slot restarted here, failed the slot
         * again and looped -- surplus harts never parked while anything
         * waited on I/O. Fork()ed DataLoader workers (cooperative pipe waits)
         * then burned the CPUs their trainer shared: lithified PyTorch
         * dataloader_train 2 vs 20 images/s (worker profile: 56 % in this
         * function, no syscalls). =0 restores the old test. */
        if (fjs_spin_quiesce_on() &&
            __atomic_load_n(&sched->teardown_quiesce, __ATOMIC_ACQUIRE))
          goto restart; /* exiting: the quiesce gate at restart lets us leave */
        if (fjs_pending_is_work() || fjs_any_child_needs_hart(sched)) {
          fjs_hart_enter_spin_update(my_vc, /*succeeded=*/1);
          if (lithe_sched_trace_enabled())
            lithe_sched_trace_emit(LITHE_ST_IDLE_SPIN, sched, 0,
                                   LITHE_ST_DEC_OK, 1);
          goto restart;
        }
      }
      /* Peek local runqueue OR any sched-wide RUNNABLE/WARM (stealable on
       * other vcores). Checking only tqsize(my_vc) missed ready peers.
       * LITHE_WAKE_HINT (GAPS B62): a woken context is queued on the hart it
       * blocked on, so every idle spinner reacting to the sched-wide count
       * herded onto one queue lock (57 % of tdequeue in a cross-hart fan-out
       * was that xchg). Like a qthreads nemesis worker, watch the OWN queue
       * first; react to work elsewhere only after it stayed queued for
       * fjs_home_grace pauses (its home hart did not take it: steal). */
      /* Tried and refuted (GAPS B62, 2026-10-26): per-L3 runnable words polled here
       * instead of the sched-wide count (the qthreads-shepherd idea applied to the
       * spin) measured 0.98-1.07x across one L3 / two L3 / two NUMA. The cross-domain
       * cost of a wake is the hops it makes, not spinners re-reading this line. */
      int other = __atomic_load_n(&sched->runnable_count, __ATOMIC_ACQUIRE) > 0;
      if (other && lithe_wake_hint_enabled() && tqsize_s(sched, my_vc) == 0) {
        if (++home_wait < fjs_home_grace)
          other = 0;
      } else {
        home_wait = 0;
      }
      if (tqsize_s(sched, my_vc) > 0 || other ||
          __atomic_load_n(&sched->warm_ready_count, __ATOMIC_ACQUIRE) > 0) {
        fjs_hart_enter_spin_update(my_vc, /*succeeded=*/1);
        if (lithe_sched_trace_enabled())
          lithe_sched_trace_emit(LITHE_ST_IDLE_SPIN, sched, 0, LITHE_ST_DEC_OK,
                                 2);
        goto restart;
      }
    }
    fjs_hart_enter_spin_update(my_vc, /*succeeded=*/0);
  }

  /* Adaptive idle park on wake_efd (pending==0): epoll_wait with a short
   * growing timeout so a small spare of idle harts stop burning empty
   * tdequeue/spinlock cycles. Infinite drive(-1) here deadlocks Finalize
   * when nobody pokes wake_efd; pending>0 still uses the infinite drive
   * above. Clearly-surplus harts (owned >> demand) skip park and yield:
   * eventfd wakes every parked sibling, so parking dozens of surplus
   * vcores stampedes them on every schedule_context (VCORE=96 P1K2).
   *
   * Critical: release hart demand before yield. schedule_context only
   * hart_request(+1); without a matching -1 on idle yield,
   * root_sched_harts_needed stays inflated and base_hart_enter immediately
   * re-grants every surplus vcore → empty hart_enter/tdequeue/spinlock
   * stampede (P1K2 VCORE=96 ~85µs vs VCORE=2 ~13µs). */
  {
    long rc = __atomic_load_n(&sched->runnable_count, __ATOMIC_ACQUIRE);
    long wc = __atomic_load_n(&sched->warm_ready_count, __ATOMIC_ACQUIRE);
    long owned = (long)parlib_atomic_read(&sched->sched.harts);
    long demand = rc + wc + 1; /* +1 spare hart for latency */
    int soft_cap;
    int is_surplus;
    fjs_tradespace_init_once();
    soft_cap = fjs_soft_hart_cap;
    if (parlib_reactor_pending() > 0)
      demand += 1;
    /* Soft-cap surplus: any hart above the rank budget is excess even when
     * demand+1 would tolerate owned==4 at VCORE=4 (the VCORE=4 churn case). */
    is_surplus = (owned > demand + fjs_surplus_slack) ||
                 (soft_cap > 0 && owned > (long)soft_cap);
    /* PARENT/SIBLING STARVATION at the idle path (same gate as the slice
     * hand-off above): an EMPTY child must not park on a hart that a starved
     * sibling or parent needs. Hosted miniFE RPH=4 x OMP=2 only worked once
     * VCORE_LIMIT >= ~RPH+2 (login-node sweep 2026-10-15): a rank blocked in
     * MPI inside its OMP child leaves that child empty, and its hart parked
     * here through the 1->64 ms escalation (~1 s) before returning, while the
     * ranks that could satisfy the wait sat RUNNABLE with zero harts. */
    if (!is_surplus && fjs_parent_starve_yield_on() && fjs_parent_starved(sched))
      is_surplus = 1;
    if (!is_surplus && fjs_idle_direct(sched))
      is_surplus = 1; /* no idle park: the hart goes up now */
    if (fjs_interfere_on()) {
      static int once;
      if (!once && __sync_bool_compare_and_swap(&once, 0, 1))
        atexit(fjs_interfere_atexit);
      fjs_sample_owned = owned;
      fjs_sample_demand = demand;
      fjs_sample_runnable = rc;
      if (is_surplus)
        __sync_fetch_and_add(&fjs_cnt_surplus_yield, 1);
    }
    if (is_surplus) {
      /* Surplus: yield immediately after demand release (below). */
    } else if (parlib_reactor_initialized()) {
      /* (only a reactor someone uses: an idle hart creating one opened three fds in every
       * process -- 2026-10-23, a file-read offload parked main at interpreter start and
       * CPython test_close_fds saw the epoll/eventfd/timerfd in exec'd children) */
      static __thread unsigned idle_park_streak;
      int my_vc = vcore_id();
idle_park_again:
      {
        int ms = 1;
        if (idle_park_streak >= 2u)
          ms = 4;
        if (idle_park_streak >= 8u)
          ms = 16;
        if (idle_park_streak >= 16u)
          ms = 64;
        if (ms > fjs_idle_park_max_ms)
          ms = fjs_idle_park_max_ms;
        if (fjs_interfere_on())
          __sync_fetch_and_add(&fjs_cnt_idle_park, 1);
        if (lithe_sched_trace_enabled())
          lithe_sched_trace_emit(LITHE_ST_REACTOR_BLOCK, sched, 0,
                                 LITHE_ST_DEC_OK, ms);
        if (fjs_reactor_drive_parked(sched, my_vc, ms)) {
          idle_park_streak = 0;
          goto restart;
        }
        if (fjs_wake_chain_on() &&
            __atomic_load_n(&sched->runnable_count, __ATOMIC_SEQ_CST) > 1 &&
            __atomic_load_n(&fjs_reactor_parked_harts, __ATOMIC_SEQ_CST) > 0)
          parlib_reactor_wake(); /* pass the drained poke on */
        if (fjs_pending_is_work() ||
            tqsize_s(sched, my_vc) > 0 ||
            __atomic_load_n(&sched->runnable_count, __ATOMIC_ACQUIRE) > 0 ||
            __atomic_load_n(&sched->warm_ready_count, __ATOMIC_ACQUIRE) > 0 ||
            __atomic_load_n(&sched->teardown_quiesce, __ATOMIC_ACQUIRE) ||
            ((idle_park_streak & 3u) == 0 &&
             fjs_any_child_needs_hart(sched))) {
          idle_park_streak = 0;
          goto restart;
        }
        /* STARVED-CHILD HEAL (parked-hart cadence): the last-exit heal at
         * the bottom of hart_enter is unreachable when reactor activity
         * (PMIx timers, event fds) keeps restarting this hart before
         * idle_park_streak runs out — it loops 1-64ms parks forever while
         * a child sits at needed==granted with RUNNABLE work (P1K4 nx=80
         * CG stall: child needed=granted=2, runnable=2, busy harts spin in
         * the CQ enter gate / cxi ep lock held by a descheduled context).
         * A parked hart is off the hot path, so probing/healing here costs
         * one child-list walk per park round at worst. */
        if ((idle_park_streak & 3u) == 0 &&
            fjs_any_child_ledger_starved(sched)) {
          fjs_heal_starved_children(sched);
          idle_park_streak = 0;
          goto restart;
        }
        if (idle_park_streak < fjs_idle_park_rounds) {
          idle_park_streak++;
          goto idle_park_again;
        }
        idle_park_streak = 0;
      }
    }
  }

  {
    long rc = __atomic_load_n(&sched->runnable_count, __ATOMIC_RELAXED);
    uint8_t dec = LITHE_ST_DEC_OK;
    if (rc > 0)
      dec |= LITHE_ST_DEC_IDLE_WITH_RUNNABLE;
    /* Rapid idle under VCORE_LIMIT=1: flag for offline rate analysis. */
    if (parlib_vcore_limit() <= 1)
      dec |= LITHE_ST_DEC_SELF_RESCHED_HOT;
    if (lithe_sched_trace_enabled())
      lithe_sched_trace_emit(LITHE_ST_IDLE_YIELD, sched, 0, dec, (int32_t)rc);
  }
  if (fjs_interfere_on())
    __sync_fetch_and_add(&fjs_cnt_idle_yield, 1);
  /* Surplus-only demand release: dropping -1 on every idle yield regresses
   * the fair VCORE≈K case (extra request/wake round-trips). When owned >>
   * demand, release so base_hart_enter will not immediately re-grant this
   * hart into an empty FJS hart_enter/tdequeue stampede. */
  {
    long rc2 = __atomic_load_n(&sched->runnable_count, __ATOMIC_RELAXED);
    long wc2 = __atomic_load_n(&sched->warm_ready_count, __ATOMIC_RELAXED);
    long owned2 = (long)parlib_atomic_read(&sched->sched.harts);
    long demand2 = rc2 + wc2 + 1;
    int soft_cap2;
    idle_yield_released_demand = 0;
    fjs_tradespace_init_once();
    soft_cap2 = fjs_soft_hart_cap;
    if (parlib_reactor_pending() > 0)
      demand2 += 1;
    if (owned2 > demand2 + 1 ||
        (soft_cap2 > 0 && owned2 > (long)soft_cap2)) {
      /* Deferred to after the LOST-WAKEUP GUARD below: releasing here and
       * then restarting either drains the needed ledger (no rebalance) or
       * pays a full lithe_hart_request(+1) tree walk + reactor-wake
       * stampede per transient-work restart (~2x P2K2 nx=80 CG). Only a
       * hart truly committed to yield releases demand. */
      idle_yield_released_demand = 1;
    }
  }
  /* LOST-WAKEUP GUARD (single-OS P1K4 nx=80 CG stall): a context enqueued
   * after our last runnable_count read but before lithe_hart_yield's harts
   * decrement gets its hart_request clamped at soft_cap (we are still
   * counted as owned) and its reactor poke is invisible to a fully-yielded
   * hart -- RUNNABLE forever while remaining harts spin inside their own
   * contexts. Protocol: publish the yield commitment (leaving_harts++,
   * SEQ_CST) FIRST, then re-check for work. Enqueue bumps runnable_count
   * (SEQ_CST, under tqlock) before its clamp reads leaving_harts, so either
   * we see the new context here and restart, or the enqueuer sees our
   * leaving credit and its hart request is granted. Never yield after
   * observing runnable work (the old path logged IDLE_WITH_RUNNABLE and
   * yielded anyway). */
fjs_idle_leave:
  __atomic_fetch_add(&sched->leaving_harts, 1, __ATOMIC_SEQ_CST);
  /* SELF-HEAL at last exit: before this hart leaves, rescue any child whose
   * ledger drifted to needed <= granted while RUNNABLE/warm work is queued
   * (fjs_heal_starved_children lifts needed; the restart pass grants). Off
   * the hot path by construction — only harts about to yield pay it. */
  if (fjs_any_child_ledger_starved(sched)) {
    fjs_heal_starved_children(sched);
    long lvh = __atomic_load_n(&sched->leaving_harts, __ATOMIC_SEQ_CST);
    while (lvh > 0 &&
           !__atomic_compare_exchange_n(&sched->leaving_harts, &lvh, lvh - 1,
                                        false, __ATOMIC_SEQ_CST,
                                        __ATOMIC_SEQ_CST))
      ;
    if (lithe_sched_trace_enabled())
      lithe_sched_trace_emit(LITHE_ST_IDLE_SPIN, sched, 0, LITHE_ST_DEC_OK, 4);
    goto restart;
  }
  if (__atomic_load_n(&sched->runnable_count, __ATOMIC_SEQ_CST) > 0 ||
      __atomic_load_n(&sched->warm_ready_count, __ATOMIC_SEQ_CST) > 0 ||
      tqsize_s(sched, vcore_id()) > 0 ||
      (!prog_child_leave && fjs_pending_is_work())) {
    long lv = __atomic_load_n(&sched->leaving_harts, __ATOMIC_SEQ_CST);
    while (lv > 0 &&
           !__atomic_compare_exchange_n(&sched->leaving_harts, &lv, lv - 1,
                                        false, __ATOMIC_SEQ_CST,
                                        __ATOMIC_SEQ_CST))
      ;
    if (lithe_sched_trace_enabled())
      lithe_sched_trace_emit(LITHE_ST_IDLE_SPIN, sched, 0, LITHE_ST_DEC_OK, 3);
    goto restart;
  }
  /* GAPS B60 (2026-10-25) LITHE_FJS_IDLE_TRIM (DEFAULT 1, =0 disables): a hart leaving an idle
   * CHILD scheduler trims the child's needed ledger to what it can still use -- the harts it
   * keeps plus its runnable and warm contexts. The release above fires only when owned >>
   * demand, so a child that holds one hart never corrects a leaked `needed`: libomp nested
   * mode (LITHE_OMP_FLAT=0) left needed=3360 on a team scheduler with nothing runnable, and
   * the parent's harts, which grant children before running their own work, cycled
   * parent -> empty child -> parent for ~30 s while the parent's own context stayed runnable
   * (cotenant 2x4, 4 harts: makespan 11-46 s for 0.4 s of rounds; gdb ledger + perf in
   * .build_logs/b60). Same rule as the E9 progress child. A context made runnable later
   * re-requests its hart through schedule_context. A/B n=10 (.build_logs/b60/ab.txt): libomp
   * nested 13.9 s -> 0.41 s makespan (10/10), everything else 0.985-1.002x (flat gompl/ompl,
   * gompl own-sched, hosted ranks K=8, hosted miniFE serial + MPI+OpenMP). */
  /* A scheduler whose children still want harts keeps that demand: its needed ledger carries
   * theirs (a first cut that trimmed to its own runnable count starved the libomp team
   * schedulers under the cotenant scheduler: p99 3.5-4 ms in 2/3 runs). Children trim first
   * when their own idle harts leave, then the parent may. */
  if (fjs_idle_trim_on() && sched->sched.parent &&
      sched->sched.parent->funcs == &lithe_fork_join_sched_funcs &&
      !fjs_any_child_needs_hart(sched)) {
    uint16_t *pg = (uint16_t *)&sched->sched.parent_data;
    uint16_t *pn = (uint16_t *)&sched->sched.parent_data + 1;
    long want = ((long)*pg - 1) +
                __atomic_load_n(&sched->runnable_count, __ATOMIC_SEQ_CST) +
                __atomic_load_n(&sched->warm_ready_count, __ATOMIC_SEQ_CST);
    long surplus = (long)*pn - (want > 0 ? want : 0);
    /* Only pathological inflation: a needed beyond every hart the process can have. Trimming
     * ordinary transient inflation cost hosted serial miniFE 1 % (CG 0.5755 -> 0.5811, slower
     * in 7/10 pairs, ab.txt): that slack is demand the next wake would re-request anyway. */
    if (surplus > (long)max_vcores()) {
      lithe_hart_request(-(int)surplus);
      idle_yield_released_demand = 0;
    }
  }
  /* Truly leaving: only now release surplus demand (see deferred note
   * above) so restarts never drain or churn the needed ledger. */
  if (idle_yield_released_demand) {
    lithe_hart_request(-1);
    if (fjs_interfere_on())
      __sync_fetch_and_add(&fjs_cnt_hart_request_release, 1);
  }
  vconline(vcore_id()) = false;
  lithe_hart_yield();
}

void lithe_fork_join_sched_context_block(lithe_sched_t *__this,
                                         lithe_context_t *c)
{
	if (vcore_id() < FJS_MAX_SLICE_VC)
		fjs_yield_streak[vcore_id()] = 0; /* streak = consecutive yields */
	lithe_fork_join_context_t *ctx = (void*)c;
	assert(ctx->state == FJS_CTX_RUNNING);
	fjs_lpt_stop(ctx, 0);
	ctx->state = FJS_CTX_BLOCKED;
	/* LITHE_WAKE_HINT: home = the hart it blocks on (about to go idle), so the
	 * wake queues it there and claims that hart (lithe.c DIRECTED WAKE). */
	if (lithe_wake_hint_enabled())
		ctx->preferred_vcq = vcore_id();
	fjs_stats_run_leave();
	__atomic_fetch_sub(&((lithe_fork_join_sched_t *)__this)->running_count, 1,
	                   __ATOMIC_SEQ_CST);
	fjs_vc_set_busy(0);
	if (lithe_sched_trace_enabled())
		lithe_sched_trace_emit(LITHE_ST_CTX_BLOCK, __this,
		                       (uint32_t)c->id, LITHE_ST_DEC_OK, 0);
	/* NEEDED-LEDGER DRIFT FIX: wake-side +1s can be clamped at soft_cap
	 * while this -1 was unconditional, so the parent's harts_needed
	 * drifted below true demand over clamped park/wake cycles and grants
	 * stopped with RUNNABLE work queued (hosted miniFE P1K4 nx=80: live
	 * gdb showed needed==granted with contexts queued state=RUNNABLE
	 * while busy harts spun in wait). Symmetric "credit" accounting kept
	 * needed pinned at cap for the whole run and cost ~2x P2K2 nx=80 CG;
	 * hart_enter heals (park-loop / last-exit) need an idle parent hart
	 * to visit hart_enter, which the stall states do not guarantee.
	 *
	 * Instead, release demand only when this sched has no queued
	 * RUNNABLE/warm work: if the queue is non-empty the demand this
	 * context represented is immediately re-owed to the queued work (the
	 * hart freed by this block, or a pending grant, must run it). Once
	 * queues drain, later blocks decrement normally, and any residual
	 * inflation is released by the idle-yield surplus path. */
	{
		lithe_fork_join_sched_t *sched = (lithe_fork_join_sched_t *)__this;
		if (__atomic_load_n(&sched->runnable_count, __ATOMIC_SEQ_CST) > 0 ||
		    __atomic_load_n(&sched->warm_ready_count, __ATOMIC_SEQ_CST) > 0)
			return;
	}
	lithe_hart_request(-1);
}

void lithe_fork_join_sched_context_unblock(lithe_sched_t *__this,
                                           lithe_context_t *c)
{
	lithe_fork_join_context_t *ctx = (void*)c;
	assert(ctx->state == FJS_CTX_BLOCKED);
	if (lithe_sched_trace_enabled())
		lithe_sched_trace_emit(LITHE_ST_CTX_UNBLOCK, __this,
		                       (uint32_t)c->id, LITHE_ST_DEC_OK, 0);
	schedule_context(ctx, false);
}

/* lithe_fork_join_context_yield_ranked: per-vcore flag read by the yield callback,
 * which runs on the same vcore right after the context switches out. */
static int fjs_yield_ranked[FJS_MAX_SLICE_VC];
void lithe_fork_join_context_yield_ranked(void)
{
	int v = vcore_id();
	if (v >= 0 && v < FJS_MAX_SLICE_VC)
		fjs_yield_ranked[v] = 1;
	lithe_context_yield();
}

static int fjs_yield_request = -1;
static inline int fjs_yield_request_on(void)
{
	if (__builtin_expect(fjs_yield_request < 0, 0)) {
		const char *e = getenv("LITHE_FJS_YIELD_REQUEST");
		fjs_yield_request = (e && e[0] == '1');
	}
	return fjs_yield_request;
}

void lithe_fork_join_sched_context_yield(lithe_sched_t *__this,
                                         lithe_context_t *c)
{
	lithe_fork_join_context_t *ctx = (void*)c;
	assert(ctx->state == FJS_CTX_RUNNING);
	{
		int v = vcore_id(), ranked = 0;
		if (v >= 0 && v < FJS_MAX_SLICE_VC && fjs_yield_ranked[v]) {
			fjs_yield_ranked[v] = 0;
			ranked = 1;
		}
		fjs_lpt_stop(ctx, ranked ? 0 : 1);
		if (v >= 0 && v < FJS_JY_MAX_VC)
			fjs_just_yielded[v] = ctx;
	}
	fjs_stats_run_leave();
	__atomic_fetch_sub(&((lithe_fork_join_sched_t *)__this)->running_count, 1,
	                   __ATOMIC_SEQ_CST);
	fjs_vc_set_busy(0);
	if (lithe_sched_trace_enabled()) {
		lithe_fork_join_sched_t *s = (lithe_fork_join_sched_t *)__this;
		long rc = __atomic_load_n(&s->runnable_count, __ATOMIC_RELAXED);
		uint8_t dec = LITHE_ST_DEC_OK;
		/* After enqueue below, peers waiting include this context; flag if
		 * others were already runnable (starvation / yield-instead-of-park). */
		if (rc > 0)
			dec |= LITHE_ST_DEC_YIELD_WITH_PEERS;
		lithe_sched_trace_emit(LITHE_ST_CTX_YIELD, __this,
		                       (uint32_t)c->id, dec, (int32_t)rc);
	}
	/* Enqueue only (no hart_request): the yielding hart reenters. Siblings
	 * may be in timed idle park on wake_efd; without a poke they wait out
	 * the timeout (HOSTED_FULLCORE soft_cap idle → ms-scale floors). */
	__thread_enqueue(ctx, false);
	fjs_maybe_reactor_wake();
	/* LITHE_FJS_YIELD_REQUEST=1 (default 0, 2026-10-24): a yield is not
	 * demand-aware -- it never requests a hart, so when waiters yield instead
	 * of blocking (libgomp GOMP_LITHE_SPIN_YIELD) no context_unblock ever
	 * issues the hart_request a park/wake would, and a hart that went back to
	 * parlib in a quiet moment never returns: climml l1f ran 4 contexts on 3
	 * harts for the rest of the run (trace: hart 3 out of the scheduler), 2x
	 * dynamics, bimodal by whether a hart had parked. Request one hart when
	 * runnable + running exceeds the harts owned. */
	if (fjs_yield_request_on()) {
		lithe_fork_join_sched_t *s = (lithe_fork_join_sched_t *)__this;
		long want = __atomic_load_n(&s->runnable_count, __ATOMIC_ACQUIRE)
		          + __atomic_load_n(&s->running_count, __ATOMIC_ACQUIRE);
		if (want > (long)parlib_atomic_read(&s->sched.harts))
			lithe_hart_request(1);
	}
	if (fjs_parent_starve_yield_on()) {
		int vc = vcore_id();
		if (vc < FJS_MAX_SLICE_VC)
			fjs_yield_streak[vc]++;
	}
}

void lithe_fork_join_sched_context_exit(lithe_sched_t *__this,
                                        lithe_context_t *c)
{
  lithe_fork_join_sched_t *sched = (void *)__this;
  lithe_fork_join_context_t *ctx = (void*)c;
  assert(ctx->state == FJS_CTX_RUNNING);
  fjs_stats_run_leave();
  __atomic_fetch_sub(&sched->running_count, 1, __ATOMIC_SEQ_CST);
  fjs_vc_set_busy(0);
  if (c != sched->sched.main_context) {
    /* NEEDED-LEDGER DRIFT FIX: same rule as context_block — keep the
     * demand while RUNNABLE/warm work is queued (see comment there). */
    if (__atomic_load_n(&sched->runnable_count, __ATOMIC_SEQ_CST) <= 0 &&
        __atomic_load_n(&sched->warm_ready_count, __ATOMIC_SEQ_CST) <= 0)
      lithe_hart_request(-1);
    int daemon = __atomic_load_n(&ctx->daemon, __ATOMIC_ACQUIRE);
    lithe_fork_join_context_destroy(ctx);
    if (!daemon)
      lithe_fork_join_sched_join_one(sched);
  }
}

/* fork() child (lithe.c lithe_fork_child; 2026-10-22). Reset this fork-join
 * scheduler and, recursively, its children: no queued context survives (they
 * were the parent's; the forker is running, not queued), hart counts follow the
 * one surviving hart -- owned by `cur`, the forker's scheduler, with every edge
 * on the chain root..cur granted one -- and the join counters forget contexts
 * that will never exit here. Non-FJS children are left alone. */
static int fjs_on_chain(lithe_sched_t *s, lithe_sched_t *cur)
{
	for (lithe_sched_t *p = cur; p; p = p->parent)
		if (p == s)
			return 1;
	return 0;
}

static void fjs_after_fork_one(lithe_fork_join_sched_t *s, lithe_sched_t *cur,
                               int vcoreid, lithe_context_t *keep)
{
	int on = fjs_on_chain(&s->sched, cur);
	int mv = (int)max_vcores();
	for (int i = 0; i < mv; i++) {
		TAILQ_INIT(&s->vc_mgmt[i].tqueue);
		spin_pdr_init(&s->vc_mgmt[i].tqlock);
		s->vc_mgmt[i].tqsize = 0;
		s->vc_mgmt[i].vconline = on && i == vcoreid;
		s->vc_mgmt[i].warm_ctx = NULL;
	}
	s->runnable_count = 0;
	s->idle_spinning = 0;
	s->warm_ready_count = 0;
	s->leaving_harts = 0;
	s->granting_harts = 0;
	s->lpt_ranked_queued = 0;
	s->running_count = (&s->sched == cur) ? 1 : 0;
	s->sched.harts = (parlib_atomic_t)PARLIB_ATOMIC_INITIALIZER(&s->sched == cur ? 1 : 0);
	s->num_contexts = 1;   /* the scheduler's main context */
	if (keep && keep->sched == &s->sched && keep != s->sched.main_context &&
	    !((lithe_fork_join_context_t *)keep)->daemon)
		s->num_contexts = 2;
	spin_pdr_init(&s->child_sched_list_lock);
	lithe_sched_t *c;
	TAILQ_FOREACH(c, &s->child_sched_list, link) {
		uint16_t *granted = (uint16_t *)&c->parent_data;
		uint16_t *needed = (uint16_t *)&c->parent_data + 1;
		*granted = *needed = fjs_on_chain(c, cur) ? 1 : 0;
		if (c->funcs == &lithe_fork_join_sched_funcs)
			fjs_after_fork_one((lithe_fork_join_sched_t *)c, cur, vcoreid, keep);
	}
}

void lithe_fork_join_after_fork(lithe_sched_t *root, lithe_sched_t *cur,
                                int vcoreid, lithe_context_t *keep)
{
	/* Process-wide hart state belongs to the parent's harts (2026-10-23):
	 * a hart parked as the reactor driver at fork time left
	 * fjs_reactor_drivers = 1 in the child, so the child's harts saw the one
	 * driver slot taken forever, never drove the reactor, and spun in
	 * hart_enter while every cooperative fd wait went unanswered (lithified
	 * CPython: Manager-hosted Pool workers forked while faulthandler's
	 * watchdog parked the driver, test_manager test_apply, 99 % CPU each). */
	fjs_reactor_drivers = 0;
	fjs_reactor_parked_harts = 0;
	fjs_running = 0;
	memset(fjs_vc_busy, 0, sizeof(fjs_vc_busy));
	memset(fjs_yield_streak, 0, sizeof(fjs_yield_streak));
	if (root && root->funcs == &lithe_fork_join_sched_funcs)
		fjs_after_fork_one((lithe_fork_join_sched_t *)root, cur, vcoreid, keep);
}
