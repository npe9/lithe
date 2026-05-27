/* Copyright (c) 2014 The Regents of the University of California
 * Kevin Klues <klueska@cs.berkeley.edu>
 * Andrew Waterman <waterman@cs.berkeley.edu>
 * See COPYING for details.
 */

#include <sys/mman.h>
#include <stdlib.h>
#include <parlib/waitfreelist.h>
#include <parlib/reactor.h>
#include <parlib/arch.h>
#include "fork_join_sched.h"
#include "lithe.h"
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
	if (parlib_reactor_initialized())
		(void)parlib_reactor_drive(0);
}

static struct wfl sched_zombie_list = WFL_INITIALIZER(sched_zombie_list);
static struct wfl context_zombie_list = WFL_INITIALIZER(context_zombie_list);

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

static void __ctx_free(lithe_fork_join_context_t *ctx)
{
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
	spin_pdr_unlock(&tqlock_s(sched, vcoreid));

	return vcoreid;
}

static int __thread_enqueue(lithe_fork_join_context_t *ctx, bool athead)
{
	assert(lithe_sched_current() == ctx->context.sched);
	return __thread_enqueue_on((lithe_fork_join_sched_t *)ctx->context.sched,
	                           ctx, athead);
}

static void schedule_context(lithe_fork_join_context_t *ctx, bool athead)
{
	__thread_enqueue(ctx, athead);
	lithe_hart_request(1);
	/* Wake any sibling vcore parked in parlib_reactor_drive(-1). lithe's
	 * own hart-grant pathway would normally do this via the parent
	 * scheduler's kernel-level wakeup, but a vcore parked in epoll_wait
	 * doesn't see those signals. The wake_efd registered in the central
	 * reactor pops them out so they can pick up the work we just queued
	 * (or yield the hart back to the parent if we get there first). */
	if (parlib_reactor_pending() > 0)
		parlib_reactor_wake();
}

static lithe_fork_join_context_t *__thread_dequeue()
{
	inline lithe_fork_join_context_t *tdequeue(int vcoreid)
	{
		lithe_fork_join_context_t *ctx = NULL;
		if (tqsize(vcoreid)) {
			spin_pdr_lock(&tqlock(vcoreid));
			ctx = (lithe_fork_join_context_t*) TAILQ_FIRST(&tqueue(vcoreid));
			if (ctx) {
				TAILQ_REMOVE(&tqueue(vcoreid), &ctx->context, link);
				tqsize(vcoreid)--;
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

	/* If there isn't one, try and steal one from someone else's queue. */
	if (!ctx) {

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
  }

  memset(main_context, 0, sizeof(*main_context));
  main_context->state = FJS_CTX_RUNNING;
  main_context->preferred_vcq = vcore_id();
  sched->sched.main_context = &main_context->context;

  sched->num_contexts = 1;
  sched->granting_harts = 0;
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

  /* Otherwise, if I have any contexts to run, grab one and run it. */
  fjs_reactor_poll();
  lithe_fork_join_context_t *ctx = __thread_dequeue();
  if (ctx != NULL) {
    assert(ctx->state == FJS_CTX_RUNNABLE);
    ctx->state = FJS_CTX_RUNNING;
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
	__thread_enqueue(ctx, false);
}

void lithe_fork_join_sched_context_exit(lithe_sched_t *__this,
                                        lithe_context_t *c)
{
  lithe_fork_join_sched_t *sched = (void *)__this;
  lithe_fork_join_context_t *ctx = (void*)c;
  assert(ctx->state == FJS_CTX_RUNNING);
  if (c != sched->sched.main_context) {
    lithe_hart_request(-1);
    lithe_fork_join_context_destroy(ctx);
    lithe_fork_join_sched_join_one(sched);
  }
}
