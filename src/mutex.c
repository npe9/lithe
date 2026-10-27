/* Copyright (c) 2011 The Regents of the University of California
 * Ben Hindman <benh@cs.berkeley.edu>
 * Kevin Klues <klueska@cs.berkeley.edu>
 * See COPYING for details.
 */

/**
 * Implementation of non-reentrant, non-error catching, unfair
 * mutexes.
 */

#include <errno.h>
#include "internal/assert.h"
#include <stdbool.h>
#include <stdlib.h>

#include <parlib/parlib.h>
#include <parlib/mcs.h>
#include "mutex.h"
#include <sched.h>

int lithe_mutexattr_init(lithe_mutexattr_t *attr)
{
  if(attr == NULL)
    return EINVAL;
  attr->type = LITHE_MUTEX_DEFAULT;
  return 0;
}

int lithe_mutexattr_settype(lithe_mutexattr_t *attr, int type)
{
  if(attr == NULL)
    return EINVAL;
  if(type >= NUM_LITHE_MUTEX_TYPES)
	return EINVAL;
  attr->type = type;
  return 0;
}

int lithe_mutexattr_gettype(lithe_mutexattr_t *attr, int *type)
{
  if(attr == NULL)
    return EINVAL;
  *type = attr->type;
  return 0;
}

int lithe_mutex_init(lithe_mutex_t *mutex, lithe_mutexattr_t *attr)
{
  if(mutex == NULL)
    return EINVAL;
  if(attr == NULL)
    lithe_mutexattr_init(&mutex->attr);
  else
    mutex->attr = *attr;

  /* Do initialization. */
  TAILQ_INIT(&mutex->queue);
  mcs_pdr_init(&mutex->lock);
  mutex->qnode = NULL;
  mutex->locked = 0;
  mutex->owner = NULL;
  return 0;
}

/*
 * Static initializers (e.g. OPAL_THREAD_INTERNAL_MUTEX_INITIALIZER) often set
 * queue={NULL,NULL}. TAILQ_INSERT_TAIL requires tqh_last=&tqh_first; a NULL
 * tqh_last SEGVs on the first contended wait (seen on OMPI wait_sync_lock under
 * hosted multi-context Barrier/Allreduce). Repair under the MCS lock.
 */
static inline void lithe_mutex_ensure_queue(lithe_mutex_t *mutex)
{
  if (mutex->queue.tqh_last == NULL) {
    TAILQ_INIT(&mutex->queue);
  }
}

static void block(lithe_context_t *context, void *arg)
{
  lithe_mutex_t *mutex = (lithe_mutex_t *) arg;
  assert(mutex);
  lithe_mutex_ensure_queue(mutex);
  TAILQ_INSERT_TAIL(&mutex->queue, context, link);
  mcs_pdr_unlock(&mutex->lock, mutex->qnode);
}

/*
 * LITHE_MUTEX_FAST (2026-10-26, default ON since the B67 A/B, =0 restores; spin audit, GAPS B67).
 * Every lock, unlock and trylock took the internal MCS pdr lock (two notif toggles
 * and two MCS cycles per lock/unlock pair, ~12 locked ops) even uncontended; libfabric's
 * CQ/EP/MR-cache mutexes made that ~50 % of a hosted 4x4 allreduce (perf). Fast mode,
 * for non-recursive mutexes only, makes `locked` a state word -- 0 free, 1 locked,
 * 2 locked and maybe waiters (Drepper's futex mutex) -- and takes the MCS lock only
 * to queue or wake a waiter:
 *   lock:   CAS 0->1; else bounded poll, then under MCS xchg(2): 0 means ours,
 *           otherwise queue and block (the block callback drops MCS). A woken waiter
 *           retries only with xchg(2), never CAS 0->1, so a later unlock still wakes
 *           the rest of the queue.
 *   unlock: xchg(0); if it was 2, under MCS pop one waiter and unblock it.
 * The xchg(2) and the enqueue happen under MCS, and an unlocker that saw 2 takes MCS
 * before popping, so it cannot miss a waiter that is mid-enqueue.
 */
static int lithe_mutex_fast = -1;
static inline int mutex_fast(const lithe_mutex_t *mutex)
{
  if (__builtin_expect(lithe_mutex_fast < 0, 0)) {
    const char *e = getenv("LITHE_MUTEX_FAST");
    lithe_mutex_fast = (e && e[0] == '0') ? 0 : 1;
  }
  return lithe_mutex_fast && mutex->attr.type != LITHE_MUTEX_RECURSIVE;
}

static int mutex_spin_budget(void)
{
  static int spin = -1;
  if (spin < 0) {
    const char *e = getenv("LITHE_MUTEX_SPIN");
    int v = e ? atoi(e) : 256;
    spin = v > 0 ? v : 0;
  }
  return spin;
}

static int fast_lock_slow(lithe_mutex_t *mutex)
{
  int woken = 0;
  for (;;) {
    int spin = mutex_spin_budget();
    for (int i = 0; i < spin && __atomic_load_n(&mutex->locked, __ATOMIC_RELAXED); i++)
      cpu_relax();
    if (!woken) {
      int z = 0;
      if (__atomic_compare_exchange_n(&mutex->locked, &z, 1, false,
                                      __ATOMIC_ACQUIRE, __ATOMIC_RELAXED))
        break;
    }
    if (!lithe_can_block()) {
      /* Not a Lithe context (GAPS B29): poll with kernel yields, never queue. */
      while (__atomic_load_n(&mutex->locked, __ATOMIC_ACQUIRE))
        sched_yield();
      if (__atomic_exchange_n(&mutex->locked, 2, __ATOMIC_ACQUIRE) == 0)
        break;
      continue;
    }
    mcs_lock_qnode_t qnode = {0};
    mcs_pdr_lock(&mutex->lock, &qnode);
    lithe_mutex_ensure_queue(mutex);
    if (__atomic_exchange_n(&mutex->locked, 2, __ATOMIC_ACQUIRE) == 0) {
      mcs_pdr_unlock(&mutex->lock, &qnode);
      break;
    }
    mutex->qnode = &qnode;
    lithe_context_block(block, mutex);
    woken = 1;
  }
  mutex->owner = lithe_context_self();
  return 0;
}

static int fast_unlock_wake(lithe_mutex_t *mutex)
{
  mcs_lock_qnode_t qnode = {0};
  mcs_pdr_lock(&mutex->lock, &qnode);
  lithe_mutex_ensure_queue(mutex);
  lithe_context_t *context = TAILQ_FIRST(&mutex->queue);
  if (context)
    TAILQ_REMOVE(&mutex->queue, context, link);
  mcs_pdr_unlock(&mutex->lock, &qnode);
  if (context)
    lithe_context_unblock(context);
  return 0;
}

int lithe_mutex_trylock(lithe_mutex_t *mutex)
{
  if(mutex == NULL)
    return EINVAL;
  if (mutex_fast(mutex)) {
    int z = 0;
    if (__atomic_compare_exchange_n(&mutex->locked, &z, 1, false,
                                    __ATOMIC_ACQUIRE, __ATOMIC_RELAXED)) {
      mutex->owner = lithe_context_self();
      return 0;
    }
    return EBUSY;
  }

  int retval = 0;
  mcs_lock_qnode_t qnode = {0};
  mcs_pdr_lock(&mutex->lock, &qnode);
  lithe_mutex_ensure_queue(mutex);
  if(mutex->attr.type == LITHE_MUTEX_RECURSIVE &&
     mutex->owner == lithe_context_self()) {
    mutex->locked++;
  }
  else if(mutex->locked) {
    retval = EBUSY;
  }
  else {
    mutex->owner = lithe_context_self();
    mutex->locked++;
  }
  mcs_pdr_unlock(&mutex->lock, &qnode);
  return retval;
}

int lithe_mutex_lock(lithe_mutex_t *mutex)
{
  if(mutex == NULL)
    return EINVAL;
  if (mutex_fast(mutex)) {
    int z = 0;
    if (__atomic_compare_exchange_n(&mutex->locked, &z, 1, false,
                                    __ATOMIC_ACQUIRE, __ATOMIC_RELAXED)) {
      mutex->owner = lithe_context_self();
      return 0;
    }
    return fast_lock_slow(mutex);
  }

  mcs_lock_qnode_t qnode = {0};
  mcs_pdr_lock(&mutex->lock, &qnode);
  lithe_mutex_ensure_queue(mutex);
  if(mutex->attr.type == LITHE_MUTEX_RECURSIVE &&
     mutex->owner == lithe_context_self()) {
    mutex->locked++;
  }
  else {
    while(mutex->locked) {
      if(!lithe_can_block()) {
        /* A caller that cannot block as a Lithe context -- a plain kernel thread
         * (parlib offload helpers running library calls, GAPS B29: a lithified
         * libfabric AV lock contended by two helpers closing endpoints crashed
         * in lithe_context_block with no current context), the base scheduler,
         * a constructor or _dl_fini -- waits by yielding its kernel thread. */
        mcs_pdr_unlock(&mutex->lock, &qnode);
        /* Poll the flag WITHOUT the queue lock until it looks free: retaking the
         * MCS lock on every retry convoyed -- 8 helpers on 4 CPUs, a queued one
         * preempted, the holder stuck in lithe_mutex_unlock behind it: 1.8-3.3 s
         * finalize in 5/10 runs (gdb, GAPS B29). */
        do {
          sched_yield();
        } while (__atomic_load_n(&mutex->locked, __ATOMIC_ACQUIRE));
        memset(&qnode, 0, sizeof(mcs_lock_qnode_t));
        mcs_pdr_lock(&mutex->lock, &qnode);
        lithe_mutex_ensure_queue(mutex);
        continue;
      }
      /* LITHE_MUTEX_SPIN=n (2026-10-20, default 256; 0 = block at once): poll
       * the flag up to n times without the queue lock before blocking. The OMPI
       * request-completion mutex (ompi_sync_wait_mt) is held for a few hundred
       * ns, but a contended lock blocked the context and the unlock unblocked
       * it through the scheduler and a hart wake: 2/3 of the syscalls of a
       * hosted 8-byte allreduce. 4 reps, login node: hosted allreduce 64 KiB
       * 22.3-22.6 -> 15.2-15.5 us/op, halo 4 KiB 16.2 -> 10.5, miniFE serial
       * 32^3 CG 0.0178-0.0182 -> 0.0168-0.0174; hosted MPI+OpenMP, GPU ranks,
       * ranks K=8, nested OpenMP, PaRSEC + OpenMP unchanged. 2048/16384 no
       * better than 256. */
      {
        static int spin = -1;
        if (spin < 0) {
          const char *e = getenv("LITHE_MUTEX_SPIN");
          int v = e ? atoi(e) : 256;
          spin = v > 0 ? v : 0;
        }
        if (spin > 0) {
          mcs_pdr_unlock(&mutex->lock, &qnode);
          for (int i = 0; i < spin &&
                          __atomic_load_n(&mutex->locked, __ATOMIC_ACQUIRE); i++)
            cpu_relax();
          memset(&qnode, 0, sizeof(mcs_lock_qnode_t));
          mcs_pdr_lock(&mutex->lock, &qnode);
          lithe_mutex_ensure_queue(mutex);
          if (!mutex->locked)
            continue;
        }
      }
      mutex->qnode = &qnode;
      lithe_context_block(block, mutex);

      memset(&qnode, 0, sizeof(mcs_lock_qnode_t));
      mcs_pdr_lock(&mutex->lock, &qnode);
      lithe_mutex_ensure_queue(mutex);
    }
    mutex->owner = lithe_context_self();
    mutex->locked++;
  }
  mcs_pdr_unlock(&mutex->lock, &qnode);
  return 0;
}

int lithe_mutex_unlock(lithe_mutex_t *mutex)
{
  if(mutex == NULL)
    return EINVAL;
  if (mutex_fast(mutex)) {
    mutex->owner = NULL;
    if (__atomic_exchange_n(&mutex->locked, 0, __ATOMIC_RELEASE) == 2)
      return fast_unlock_wake(mutex);
    return 0;
  }

  mcs_lock_qnode_t qnode = {0};
  mcs_pdr_lock(&mutex->lock, &qnode);
  lithe_mutex_ensure_queue(mutex);
  mutex->locked--;
  if(mutex->locked == 0) {
    lithe_context_t *context = TAILQ_FIRST(&mutex->queue);
    if(context)
      TAILQ_REMOVE(&mutex->queue, context, link);
    mutex->owner = NULL;
    mcs_pdr_unlock(&mutex->lock, &qnode);

    if(context != NULL) {
      lithe_context_unblock(context);
    }
  }
  else {
    mcs_pdr_unlock(&mutex->lock, &qnode);
  }
  return 0;
}

