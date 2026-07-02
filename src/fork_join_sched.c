/* Copyright (c) 2014 The Regents of the University of California
 * Kevin Klues <klueska@cs.berkeley.edu>
 * Andrew Waterman <waterman@cs.berkeley.edu>
 * See COPYING for details.
 */

#include <sys/mman.h>
#include <stdlib.h>
#include <stdio.h>
#include <parlib/waitfreelist.h>
#include <parlib/reactor.h>
#include <parlib/arch.h>
#include "fork_join_sched.h"
#include "lithe.h"
#include "mutex.h"   /* WARM VCORES: park_warm atomically unlocks a lithe_mutex_t */
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

static inline void fjs_reactor_poll(void)
{
	/* parlib_reactor_drive(0) is epoll_wait(..., 0). When no uthreads are
	 * parked on fd readiness (pending==0), that syscall is pure overhead on
	 * every hart_enter restart — dominant in MPI_Init/Finalize envelope
	 * profiles at full-node rank count. hart_request still pokes wake_efd
	 * for vcores blocked in drive(-1); runqueue work is found via dequeue
	 * and the adaptive spin without needing a zero-timeout poll here. */
	if (parlib_reactor_initialized() && parlib_reactor_pending() > 0)
		(void)parlib_reactor_drive(0);
}

static struct wfl sched_zombie_list = WFL_INITIALIZER(sched_zombie_list);
static struct wfl context_zombie_list = WFL_INITIALIZER(context_zombie_list);

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
    // TODO wfl currently assumes stacksize the same for all contexts
    lithe_fork_join_context_t *ctx = wfl_remove(&context_zombie_list);
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
		void *stackbot = mmap(
			0, stacksize, PROT_READ|PROT_WRITE|PROT_EXEC,
			MAP_PRIVATE|MAP_ANONYMOUS, -1, 0
		);
		if (stackbot == MAP_FAILED)
			abort();
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
    if (wfl_size(&context_zombie_list) < 1000) {
        wfl_insert(&context_zombie_list, ctx);
    } else {
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

static int __thread_enqueue_on(lithe_fork_join_sched_t *sched,
                               lithe_fork_join_context_t *ctx, bool athead)
{
	assert(ctx->context.sched == (lithe_sched_t *)sched);
	ctx->state = FJS_CTX_RUNNABLE;

	if (ctx->preferred_vcq == -1 || !vconline_s(sched, ctx->preferred_vcq))
		ctx->preferred_vcq = get_next_queue_id_for(sched);

	int vcoreid = ctx->preferred_vcq;
	spin_pdr_lock(&tqlock_s(sched, vcoreid));
	if (athead)
		TAILQ_INSERT_HEAD(&tqueue_s(sched, vcoreid), &ctx->context, link);
	else
		TAILQ_INSERT_TAIL(&tqueue_s(sched, vcoreid), &ctx->context, link);
	tqsize_s(sched, vcoreid)++;
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
	fjs_wake_batch_harts = 0;
	fjs_wake_batch_wake = 0;
	/* One hart request for the whole team release (parent grants N harts), then
	 * one reactor poke for any vcores parked in parlib_reactor_drive(-1). */
	if (h > 0)
		lithe_hart_request(h);
	if (w && parlib_reactor_pending() > 0)
		parlib_reactor_wake();
}

/* Issue (or batch) a hart request + reactor poke. Factored out of
 * schedule_context so the WARM VCORES resume path (lithe_fork_join_resume_warm)
 * shares the exact same batched accounting: while a team-wake batch is open
 * (master's fork/join release loop) the request is deferred and collapsed into
 * a single lithe_hart_request(N) + one reactor_wake at batch end. */
static void fjs_request_harts(int h)
{
	if (fjs_wake_batch_depth > 0) {
		fjs_wake_batch_harts += h;
		fjs_wake_batch_wake = 1;
		return;
	}
	lithe_hart_request(h);
	/* Wake any sibling vcore parked in parlib_reactor_drive(-1). lithe's
	 * own hart-grant pathway would normally do this via the parent
	 * scheduler's kernel-level wakeup, but a vcore parked in epoll_wait
	 * doesn't see those signals. The wake_efd registered in the central
	 * reactor pops them out so they can pick up the work we just queued
	 * (or yield the hart back to the parent if we get there first). */
	if (parlib_reactor_pending() > 0)
		parlib_reactor_wake();
}

static void schedule_context(lithe_fork_join_context_t *ctx, bool athead)
{
	__thread_enqueue(ctx, athead);
	/* CHANGE 2: when a team-wake batch is open (master's fork/join release
	 * loop), enqueue all workers first and defer a single hart_request(N) +
	 * reactor_wake to lithe_fork_join_wake_batch_end(). */
	fjs_request_harts(1);
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

static lithe_fork_join_context_t *__thread_dequeue()
{
	lithe_fork_join_sched_t *cur = (lithe_fork_join_sched_t *)lithe_sched_current();

	inline lithe_fork_join_context_t *tdequeue(int vcoreid)
	{
		lithe_fork_join_context_t *ctx = NULL;
		if (tqsize(vcoreid)) {
			spin_pdr_lock(&tqlock(vcoreid));
			ctx = (lithe_fork_join_context_t*) TAILQ_FIRST(&tqueue(vcoreid));
			if (ctx) {
				TAILQ_REMOVE(&tqueue(vcoreid), &ctx->context, link);
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

	int vcoreid = vcore_id();
	lithe_fork_join_context_t *ctx = NULL;

	/* Try and grab a thread from our queue */
	ctx = tdequeue(vcoreid);

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
			return NULL;

		/* Steal up to half of the threads in the queue and return the first */
		lithe_fork_join_context_t *steal_threads(int vcoreid)
		{
			lithe_fork_join_context_t *ctx = NULL;
			int num_to_steal = (tqsize(vcoreid) + 1) / 2;
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

		/* First try doing power of two choices. */
		int choice[2] = { rand_r(&rseed(vcoreid)) % num_vcores(),
		                  rand_r(&rseed(vcoreid)) % num_vcores()};
		int size[2] = { tqsize(choice[0]),
		                tqsize(choice[1])};
		int id = (size[0] > size[1]) ? 0 : 1;
		if (vcoreid != choice[id])
			ctx = steal_threads(choice[id]);
		else
			ctx = steal_threads(choice[!id]);

		/* Fall back to looping through all vcores. This time I go through
		 * max_vcores() just to make sure I don't miss anything. */
		if (!ctx) {
			int i = (vcoreid + 1) % max_vcores();
			while(i != vcoreid) {
				ctx = steal_threads(i);
				if (ctx) break;
				i = (i + 1) % max_vcores();
			}
		}
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

lithe_fork_join_sched_t *lithe_ensure_root_fork_join_sched(void)
{
  /* Base bootstrap (owned by lithe/parlib, idempotent): main -> vcore 0 +
   * parlib vcore pool + base scheduler. Safe to call repeatedly. */
  lithe_ensure_main_on_vcore0();

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
  sched->runnable_count = 0;  /* CHANGE 1: O(1) any-runnable indicator */
  sched->warm_ready_count = 0;  /* WARM VCORES: GO-and-unclaimed warm workers */
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

static void block_main_context(lithe_context_t *c, void *arg)
{
  lithe_fork_join_sched_join_one(arg);
}

void lithe_fork_join_sched_join_all(lithe_fork_join_sched_t *sched)
{
  lithe_context_block(block_main_context, sched);
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

	__sync_fetch_and_add(&from->num_contexts, -1);
	lithe_context_reassociate(&ctx->context, &to->sched);
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
}

void lithe_fork_join_sched_sched_exit(lithe_sched_t *__this)
{
	vconline(vcore_id()) = false;
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
}

void lithe_fork_join_sched_child_exit(lithe_sched_t *__this,
                                      lithe_sched_t *child)
{
  lithe_fork_join_sched_t *sched = (void *)__this;
  spin_pdr_lock(&sched->child_sched_list_lock);
  TAILQ_REMOVE(&sched->child_sched_list, child, link);
  spin_pdr_unlock(&sched->child_sched_list_lock);

  while (sched->granting_harts)
    cpu_relax();
}


void lithe_fork_join_sched_hart_return(lithe_sched_t *__this,
                                       lithe_sched_t *child)
{
	uint16_t *harts_granted = (uint16_t*)&child->parent_data;
	atomic_add(harts_granted, -1);
	vconline(vcore_id()) = true;
}

static void decrement(void *gh)
{
  __sync_fetch_and_add((size_t*)gh, -1);
}

void lithe_fork_join_sched_hart_enter(lithe_sched_t *__this)
{
  lithe_fork_join_sched_t *sched = (void *)__this;
  if (!vconline(vcore_id()))
    vconline(vcore_id()) = true;

restart:
  /* Scheduler-driven netpoll: drain completed fd waits before selecting the
   * next continuation. This keeps I/O readiness in the normal Lithe handoff
   * path instead of requiring a helper pthread. */
  fjs_reactor_poll();

  /* If I have any outstanding requests from my children, preferentially pass
   * this hart down to them. */
  if (!TAILQ_EMPTY(&sched->child_sched_list)) {
    __sync_fetch_and_add(&sched->granting_harts, 1);
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

      uint16_t *harts_granted = (uint16_t*)&child->parent_data;
      uint16_t *harts_needed = (uint16_t*)&child->parent_data + 1;
      if (atomic_add(harts_granted, 1) + 1 <= *harts_needed) {
        vconline(vcore_id()) = false;
        lithe_hart_grant(child, decrement, &sched->granting_harts);
      }
      atomic_add(harts_granted, -1);

      if (first == NULL)
        first = child;
      else if (first == child)
        break;
    }
    decrement(&sched->granting_harts);
  }

  /* WARM VCORES fast path: a worker parked in-place on a vcore slot and has
   * been marked GO by the master's fork. Resume it directly -- no runqueue
   * dequeue, no work-steal scan, no per-worker re-route. Own slot first (same
   * vcore => warm cache/TLS), then a bounded scan so a worker is never stranded
   * if a different vcore got this hart. Cheap single atomic load when idle. */
  if (__atomic_load_n(&sched->warm_ready_count, __ATOMIC_ACQUIRE) > 0) {
    lithe_fork_join_context_t *w = fjs_warm_claim(sched, vcore_id());
    if (w != NULL) {
      assert(w->context.uth.tls_desc != NULL);
      w->state = FJS_CTX_RUNNING;
      fjs_stats_run_enter();
      lithe_context_run(&w->context);
    }
  }

  /* Otherwise, if I have any contexts to run, grab one and run it. */
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
    fjs_stats_run_enter();
    lithe_context_run(&ctx->context);
  }

  /* No runnable contexts. If any continuations are parked on fd readiness,
   * this scheduler handoff blocks the vcore in epoll_wait until an fd fires
   * or the reactor wake fd is poked by a newly-runnable context, then loops
   * back through the full scheduler decision path. */
  while (parlib_reactor_pending() > 0) {
    (void)parlib_reactor_drive(-1);
    goto restart;
  }

  /* Adaptive spin-then-yield: a bounded cpu_relax loop catches work that
   * becomes runnable in the few hundred ns after we drained the runqueue.
   * The check cost is two integer-load checks per pause, so a busy run loop
   * pays at most fjs_hart_enter_spin_cur[vc] PAUSE-ish cycles before
   * yielding. Per-vcore counter doubles on success (work appeared during
   * the spin) and halves on failure (had to yield anyway); bounded by
   * LITHE_FJS_HART_ENTER_SPIN_{MIN,MAX}. LITHE_FJS_HART_ENTER_SPIN_LOOPS=0
   * disables. */
  fjs_hart_enter_spin_init();
  if (fjs_hart_enter_spin_loops > 0) {
    int my_vc = vcore_id();
    unsigned int budget = fjs_hart_enter_spin_budget(my_vc);
    for (unsigned int i = 0; i < budget; i++) {
      cpu_relax();
      if (!TAILQ_EMPTY(&sched->child_sched_list) ||
          parlib_reactor_pending() > 0) {
        fjs_hart_enter_spin_update(my_vc, /*succeeded=*/1);
        goto restart;
      }
      /* Peek at this vcore's runqueue without dequeuing -- another vcore
       * may have just enqueued. __thread_dequeue inside restart will resolve
       * the race correctly (it will also try work-stealing). */
      if (tqsize_s(sched, my_vc) > 0) {
        fjs_hart_enter_spin_update(my_vc, /*succeeded=*/1);
        goto restart;
      }
    }
    fjs_hart_enter_spin_update(my_vc, /*succeeded=*/0);
  }

  vconline(vcore_id()) = false;
  lithe_hart_yield();
}

void lithe_fork_join_sched_context_block(lithe_sched_t *__this,
                                         lithe_context_t *c)
{
	lithe_fork_join_context_t *ctx = (void*)c;
	assert(ctx->state == FJS_CTX_RUNNING);
	ctx->state = FJS_CTX_BLOCKED;
	fjs_stats_run_leave();
	lithe_hart_request(-1);
}

void lithe_fork_join_sched_context_unblock(lithe_sched_t *__this,
                                           lithe_context_t *c)
{
	lithe_fork_join_context_t *ctx = (void*)c;
	assert(ctx->state == FJS_CTX_BLOCKED);
	schedule_context(ctx, false);
}

void lithe_fork_join_sched_context_yield(lithe_sched_t *__this,
                                         lithe_context_t *c)
{
	lithe_fork_join_context_t *ctx = (void*)c;
	assert(ctx->state == FJS_CTX_RUNNING);
	fjs_stats_run_leave();
	__thread_enqueue(ctx, false);
}

void lithe_fork_join_sched_context_exit(lithe_sched_t *__this,
                                        lithe_context_t *c)
{
  lithe_fork_join_sched_t *sched = (void *)__this;
  lithe_fork_join_context_t *ctx = (void*)c;
  assert(ctx->state == FJS_CTX_RUNNING);
  fjs_stats_run_leave();
  if (c != sched->sched.main_context) {
    lithe_hart_request(-1);
    lithe_fork_join_context_destroy(ctx);
    lithe_fork_join_sched_join_one(sched);
  }
}
