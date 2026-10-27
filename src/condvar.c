/* Copyright (c) 2012 The Regents of the University of California
 * Kevin Klues <klueska@cs.berkeley.edu>
 * See COPYING for details.
 */

/**
 * Implementation of non-reentrant, non-error catching, unfair condition
 * variables.
 */

#include <errno.h>
#include "internal/assert.h"
#include <stdbool.h>
#include <stdlib.h>
#include <sched.h>

#include <parlib/parlib.h>
#include "mutex.h"
#include "condvar.h"

/* Same issue as lithe_mutex: static {NULL,NULL} queue is not a valid TAILQ. */
static inline void lithe_condvar_ensure_queue(lithe_condvar_t *c)
{
  if (c->queue.tqh_last == NULL) {
    TAILQ_INIT(&c->queue);
  }
}

/* Initialize a condition variable. */
int lithe_condvar_init(lithe_condvar_t* c) {
  if(c == NULL)
    return EINVAL;

  mcs_pdr_init(&c->lock);
  TAILQ_INIT(&c->queue);
  return 0;
}

static void block(lithe_context_t *context, void *arg)
{
  lithe_condvar_t *condvar = (lithe_condvar_t *) arg;
  assert(condvar);
  lithe_condvar_ensure_queue(condvar);
  TAILQ_INSERT_TAIL(&condvar->queue, context, link);
  lithe_mutex_unlock(condvar->waiting_mutex);
  mcs_pdr_unlock(&condvar->lock, condvar->waiting_qnode);
}


/* ===================== Condvar on the parking lot (docs/PLAN-locking.md P3) ==========
 * LITHE_CONDVAR_LOT=1 (2026-10-26, default off for A/B). The 32-bit sequence word is the
 * low half of `waiting_qnode` (unused in this mode, so the layout is unchanged).
 * wait: read seq, unlock m, park on &seq while seq is unchanged, relock m.
 * signal: seq++, unpark one. broadcast: seq++, wake one and, if m is a parking-lot mutex
 * that is held, requeue the rest onto m's word (they wake one at a time as m is
 * released) instead of waking them all to fight for m. A requeued waiter may be handed
 * m by the unlocker (fairness): then it already owns m. */
#include "futex.h"
int __lithe_mutex_lot_mark_parked(lithe_mutex_t *mutex);
int __lithe_mutex_is_lot(lithe_mutex_t *mutex);
void __lithe_mutex_lot_took_handoff(lithe_mutex_t *mutex);
static int cv_lot = -1;
static inline int cv_lot_on(void)
{
  if (__builtin_expect(cv_lot < 0, 0)) {
    const char *e = getenv("LITHE_CONDVAR_LOT");
    cv_lot = (e && e[0] == '1') ? 1 : 0;
  }
  return cv_lot;
}
static inline int *cv_seq(lithe_condvar_t *c) { return (int *)&c->waiting_qnode; }
struct cv_arg { int *seq; int val; };
static int cv_validate(void *a)
{
  struct cv_arg *v = a;
  return __atomic_load_n(v->seq, __ATOMIC_ACQUIRE) == v->val;
}
static int cv_requeue_cb(void *arg, int moving)
{
  (void)moving;
  return __lithe_mutex_lot_mark_parked((lithe_mutex_t *)arg);
}
static int cv_lot_wait(lithe_condvar_t *c, lithe_mutex_t *m)
{
  struct cv_arg v = { cv_seq(c), __atomic_load_n(cv_seq(c), __ATOMIC_ACQUIRE) };
  __atomic_store_n(&c->waiting_mutex, m, __ATOMIC_RELAXED);
  lithe_mutex_unlock(m);
  long token = 0;
  int r = lithe_park(v.seq, cv_validate, &v, 0, &token);
  if (r == LITHE_PARK_NOTSUPP) {
    while (__atomic_load_n(v.seq, __ATOMIC_ACQUIRE) == v.val)
      sched_yield();
  } else if (r == LITHE_PARK_UNPARKED && token == 1 && __lithe_mutex_is_lot(m)) {
    __lithe_mutex_lot_took_handoff(m);   /* requeued onto m and handed m on unlock */
    return 0;
  }
  return lithe_mutex_lock(m);
}
static int cv_lot_signal(lithe_condvar_t *c)
{
  __atomic_fetch_add(cv_seq(c), 1, __ATOMIC_SEQ_CST);
  if (lithe_park_maybe_waiters(cv_seq(c)))
    (void)lithe_unpark_one(cv_seq(c), NULL, NULL);
  return 0;
}
static int cv_lot_broadcast(lithe_condvar_t *c)
{
  __atomic_fetch_add(cv_seq(c), 1, __ATOMIC_SEQ_CST);
  if (!lithe_park_maybe_waiters(cv_seq(c)))
    return 0;
  lithe_mutex_t *m = __atomic_load_n(&c->waiting_mutex, __ATOMIC_RELAXED);
  if (m && __lithe_mutex_is_lot(m))
    (void)lithe_unpark_requeue(cv_seq(c), &m->locked, 1, cv_requeue_cb, m);
  else
    (void)lithe_unpark_all(cv_seq(c));
  return 0;
}
/* ===================================================================================== */

/* Wait on a condition variable */
int lithe_condvar_wait(lithe_condvar_t* c, lithe_mutex_t* m) {
  if(c == NULL)
    return EINVAL;
  if(m == NULL)
    return EINVAL;
  if (cv_lot_on())
    return cv_lot_wait(c, m);

  mcs_lock_qnode_t qnode = {0};
  mcs_pdr_lock(&c->lock, &qnode);
  lithe_condvar_ensure_queue(c);
  c->waiting_mutex = m;
  c->waiting_qnode = &qnode;
  lithe_context_block(block, c);
  return lithe_mutex_lock(m);
}

/* Signal the next lithe context waiting on the condition variable */
int lithe_condvar_signal(lithe_condvar_t* c) {
  if(c == NULL)
    return EINVAL;
  if (cv_lot_on())
    return cv_lot_signal(c);

  mcs_lock_qnode_t qnode = {0};
  mcs_pdr_lock(&c->lock, &qnode);
  lithe_condvar_ensure_queue(c);
  lithe_context_t *context = TAILQ_FIRST(&c->queue);
  if(context)
    TAILQ_REMOVE(&c->queue, context, link);
  mcs_pdr_unlock(&c->lock, &qnode);

  if (context != NULL) {
    lithe_context_unblock(context);
  }
  return 0;
}

/* Broadcast a signal to all lithe contexts waiting on the condition variable */
int lithe_condvar_broadcast(lithe_condvar_t* c) {
  if(c == NULL)
    return EINVAL;
  if (cv_lot_on())
    return cv_lot_broadcast(c);

  mcs_lock_qnode_t qnode = {0};
  while(1) {
    mcs_pdr_lock(&c->lock, &qnode);
    lithe_condvar_ensure_queue(c);
    lithe_context_t *context = TAILQ_FIRST(&c->queue);
    if(context)
      TAILQ_REMOVE(&c->queue, context, link);
    else break;
    mcs_pdr_unlock(&c->lock, &qnode);
    lithe_context_unblock(context);
    memset(&qnode, 0, sizeof(mcs_lock_qnode_t));
  }
  mcs_pdr_unlock(&c->lock, &qnode);
  return 0;
}

