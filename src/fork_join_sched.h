/* Copyright (c) 2014 The Regents of the University of California
 * Kevin Klues <klueska@cs.berkeley.edu>
 * Andrew Waterman <waterman@cs.berkeley.edu>
 * See COPYING for details.
 */

/*
 * Simple fork-join scheduler.
 */

#ifndef LITHE_FORK_JOIN_SCHED_H
#define LITHE_FORK_JOIN_SCHED_H

#include "sched.h"
#include "context.h"
#include <parlib/waitfreelist.h>
#include <parlib/spinlock.h>

#ifdef __cplusplus
extern "C" {
#endif

extern const lithe_sched_funcs_t lithe_fork_join_sched_funcs;

/* Stack stuff. */
#define FJS_STACK_PAGES 1024
#define FJS_STACK_SIZE (FJS_STACK_PAGES*PGSIZE)

/* FJS_CTX states. */
#define FJS_CTX_CREATED			1
#define FJS_CTX_RUNNABLE		2
#define FJS_CTX_RUNNING			3
#define FJS_CTX_BLOCKED			4

/* WARM VCORES (warm_state values). A worker that parks at an OpenMP fork/join
 * region boundary records itself in a per-vcore "warm slot" so the next fork
 * can resume it IN PLACE from hart_enter -- no runqueue enqueue/dequeue, no
 * work-steal scan, no per-worker re-route through context_unblock. The slot is
 * single-deep per vcore; if it is already occupied (rare: >1 worker cycling on
 * one vcore) the parking worker falls back to the normal runqueue (OVERFLOW),
 * which is the proven cooperative path. See lithe_fork_join_park_warm /
 * lithe_fork_join_resume_warm in fork_join_sched.c. */
#define FJS_WARM_NONE			0
#define FJS_WARM_SLOT			1
#define FJS_WARM_OVERFLOW		2

struct lithe_fork_join_vc_mgmt {
	struct lithe_context_queue tqueue;
	spin_pdr_lock_t tqlock;
	int tqsize;
	unsigned int rseed;
	bool vconline;
	/* WARM VCORES: the single worker context parked-in-place on this vcore
	 * (NULL when none). Claimed (CAS->NULL) by hart_enter to resume it without
	 * touching the runqueue. Plain C atomics only (no Linux primitives above
	 * parlib); FJS-internal so appending is ABI-safe (callers hold opaque
	 * pointers + the offset-0 lithe_sched_t). */
	lithe_context_t *warm_ctx;
} __attribute__((aligned(ARCH_CL_SIZE)));
#define tqueue_s(sched, i)   (sched)->vc_mgmt[(i)].tqueue
#define tqlock_s(sched, i)   (sched)->vc_mgmt[(i)].tqlock
#define tqsize_s(sched, i)   (sched)->vc_mgmt[(i)].tqsize
#define rseed_s(sched, i)    (sched)->vc_mgmt[(i)].rseed
#define vconline_s(sched, i) (sched)->vc_mgmt[(i)].vconline
#define tqueue(i)   tqueue_s((lithe_fork_join_sched_t*)lithe_sched_current(), i)
#define tqlock(i)   tqlock_s((lithe_fork_join_sched_t*)lithe_sched_current(), i)
#define tqsize(i)   tqsize_s((lithe_fork_join_sched_t*)lithe_sched_current(), i)
#define rseed(i)    rseed_s((lithe_fork_join_sched_t*)lithe_sched_current(), i)
#define vconline(i) vconline_s((lithe_fork_join_sched_t*)lithe_sched_current(), i)

typedef struct {
  lithe_sched_t sched;
  size_t num_contexts;
  size_t granting_harts;
  volatile int next_queue_id;
  struct lithe_sched_queue child_sched_list;
  spin_pdr_lock_t child_sched_list_lock;
  struct lithe_fork_join_vc_mgmt *vc_mgmt;
  /* CHANGE 1 (O(1) dispatch): total RUNNABLE contexts currently sitting on this
   * scheduler's per-vcore runqueues (sum of tqsize over all vcores), maintained
   * as a single atomic. A vcore in hart_enter whose own queue is empty reads
   * this once: if 0, NO queue can have work, so it skips the O(max_vcores())
   * work-steal scan and yields immediately. Incremented under tqlock on every
   * enqueue and decremented under tqlock on every dequeue, so it is always >=
   * the number of contexts an observer could find in the queues. Every net-new
   * enqueue is followed by lithe_hart_request (schedule_context), and the yield
   * re-enqueue keeps the count > 0 for the same vcore, so a stale-0 reader is
   * always followed by another hart_enter that observes the updated count --
   * no lost wakeup / hang. Plain C11 atomics only (no Linux primitives above
   * parlib); the field is FJS-internal (callers only hold opaque pointers and
   * the offset-0 embedded lithe_sched_t), so appending it is ABI-safe for
   * libomp/OMPI which link liblithe.so dynamically. */
  long runnable_count;
  /* WARM VCORES: number of warm-parked workers (FJS_WARM_SLOT) that have been
   * marked GO by a fork (lithe_fork_join_resume_warm) and not yet claimed by a
   * hart_enter. A vcore reads this once: if 0, no warm worker is resumable, so
   * the warm fast-path in hart_enter is skipped entirely. Maintained with the
   * same release/acquire discipline as runnable_count (incremented before the
   * paired hart_request, decremented on claim), so a hart woken for a warm
   * resume always observes count>0 and finds the worker -- no lost wakeup. */
  long warm_ready_count;
} lithe_fork_join_sched_t;

typedef struct {
  lithe_context_t context;
  uint32_t state;
  int preferred_vcq;
  void (*start_routine)(void*);
  void *arg;
  int stack_offset;
  /* WARM VCORES per-context state (see FJS_WARM_* above). warm_state records
   * whether this context is parked in a vcore slot or overflowed to the
   * runqueue; warm_go is the resumable flag flipped by resume_warm and cleared
   * on claim; warm_vcq is the slot's vcore (-1 if none). warm_sched is the
   * scheduler whose vc_mgmt[warm_vcq] holds this context at park time: lithified
   * libomp MIGRATES worker contexts between fork-join schedulers (creation sched
   * -> sticky top sched), so resume_warm must compare warm_sched with the
   * context's CURRENT sched. When they differ the slot was stranded in a now-
   * defunct sched; resume clears it and falls back to the runqueue. When they
   * match (steady state) the in-place fast path applies. */
  int warm_state;
  volatile int warm_go;
  int warm_vcq;
  void *warm_sched;
} lithe_fork_join_context_t;


/* API to request harts and make sure they are tracked properly when
 * "inheriting" from the lithe_fork_join_sched.  You should call this instead
 * of calling lithe_hart_request() directly. */
void lithe_fork_join_hart_request_inc(lithe_fork_join_sched_t *sched, int h);

/* CHANGE 2 (O(1) team broadcast wake): bracket a burst of context unblocks
 * (e.g. an OpenMP team fork/join release that wakes all N workers) so the N
 * individual lithe_hart_request(1) + reactor_wake calls collapse into a single
 * lithe_hart_request(N) + one reactor_wake at batch end. Must be called as a
 * begin/end pair on the SAME context with no context switch in between (the
 * release loop runs synchronously on the master). Nestable; a missing end is a
 * no-op for the non-batched path. Used by lithified libomp's barrier release. */
void lithe_fork_join_wake_batch_begin(void);
void lithe_fork_join_wake_batch_end(void);

/* WARM VCORES (persistent worker<->vcore affinity for OpenMP fork/join).
 *
 * lithe_fork_join_park_warm(mx): called by an OMP worker context (running on
 * its vcore) when it reaches a region-boundary barrier and would cooperatively
 * block. Like lithe_condvar_wait it atomically blocks the context and unlocks
 * the caller-held mutex `mx` (an opaque lithe_mutex_t*; pass NULL for none),
 * then RE-LOCKS `mx` when the worker is resumed. The difference from a condvar
 * is dispatch: the worker is recorded in its vcore's warm slot and the hart is
 * returned to the parent (hart_request(-1), for composition), so the next fork
 * resumes it IN PLACE from hart_enter with no runqueue enqueue/dequeue, no
 * work-steal scan, and no per-worker context_unblock.
 *
 * lithe_fork_join_resume_warm(ctx): called by the master (release side, inside
 * the wake batch) to resume a warm-parked worker: one shared go-flag write + a
 * single (batched) hart_request -- not a per-worker condvar signal + enqueue.
 * MUST be called with lithe_sched_current() == ctx->sched (true for the OMP
 * fork/join release, where the master has entered the sticky top FJ sched).
 *
 * C atomics + lithe primitives only (no Linux sync above parlib); preserves the
 * per-runtime FJ scheduler and the parent/child hart-grant tree. */
void lithe_fork_join_park_warm(void *mx);
void lithe_fork_join_resume_warm(lithe_context_t *ctx);

/* General, runtime-combination-agnostic root fork-join scheduler bootstrap.
 * Returns a single process-wide root FJ scheduler entered under the base
 * scheduler, bootstrapping main->vcore0 + the parlib vcore pool (owned by
 * lithe/parlib) on first use. Idempotent and safe to call from any lithified
 * runtime (libomp, OPAL/MPI, PMIx, UCX/OFI); the first runtime to init creates
 * and enters the root, later runtimes get the same one and compose their own
 * child schedulers under it. See the lithe-bootstrap-any-runtime-combination
 * rule. NULL only if scheduler allocation failed. */
lithe_fork_join_sched_t *lithe_ensure_root_fork_join_sched(void);
/* Whether the root FJ sched has been entered (non-zero) yet. */
int lithe_root_fork_join_sched_entered(void);

/* Scheduler creation, initialization, etc. for the lithe_fork_join_sched. */
lithe_fork_join_sched_t *lithe_fork_join_sched_create();
void lithe_fork_join_sched_init(lithe_fork_join_sched_t *sched,
                                lithe_fork_join_context_t *main_context);
void lithe_fork_join_sched_cleanup(lithe_fork_join_sched_t *sched);
void lithe_fork_join_sched_destroy(lithe_fork_join_sched_t *sched);

/* Context creation, initialization, etc. for the lithe_fork_join_sched. */
lithe_fork_join_context_t*
  lithe_fork_join_context_create(lithe_fork_join_sched_t *sched,
                                 size_t stack_size,
                                 void (*start_routine)(void*),
                                 void *arg);
void lithe_fork_join_context_init(lithe_fork_join_sched_t *sched,
                                  lithe_fork_join_context_t *ctx,
                                  void (*start_routine)(void*),
                                  void *arg);
void lithe_fork_join_context_cleanup(lithe_fork_join_context_t *context);
void lithe_fork_join_context_destroy(lithe_fork_join_context_t *context);
void lithe_fork_join_sched_join_one(lithe_fork_join_sched_t *sched);
void lithe_fork_join_sched_join_all(lithe_fork_join_sched_t *sched);

/* Move a fork-join context between schedulers, updating runnable queues and
 * num_contexts. Required when re-homing OpenMP workers: lithe_context_reassociate
 * alone leaves stale TAILQ links on the old sched's per-vcore queues. */
void lithe_fork_join_context_migrate(lithe_fork_join_sched_t *from,
                                     lithe_fork_join_sched_t *to,
                                     lithe_fork_join_context_t *ctx);

/* Callback implementations that can be used by schedulers that "inherit" from
 * the lithe_fork_join_sched. */
void lithe_fork_join_sched_hart_request(lithe_sched_t *__this,
                                        lithe_sched_t *child,
                                        int h);
void lithe_fork_join_sched_sched_enter(lithe_sched_t *__this);
void lithe_fork_join_sched_sched_exit(lithe_sched_t *__this);
void lithe_fork_join_sched_child_enter(lithe_sched_t *__this,
                                       lithe_sched_t *child);
void lithe_fork_join_sched_child_exit(lithe_sched_t *__this,
                                      lithe_sched_t *child);
void lithe_fork_join_sched_hart_return(lithe_sched_t *__this,
                                       lithe_sched_t *child);
void lithe_fork_join_sched_hart_enter(lithe_sched_t *__this);
void lithe_fork_join_sched_context_block(lithe_sched_t *__this,
                                         lithe_context_t *c);
void lithe_fork_join_sched_context_unblock(lithe_sched_t *__this,
                                           lithe_context_t *c);
void lithe_fork_join_sched_context_yield(lithe_sched_t *__this,
                                         lithe_context_t *c);
void lithe_fork_join_sched_context_exit(lithe_sched_t *__this,
                                        lithe_context_t *c);

#ifdef __cplusplus
}
#endif

#endif  // LITHE_FORK_JOIN_SCHED_H
