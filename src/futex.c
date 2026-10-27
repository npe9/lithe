#include <sys/queue.h>
#include <parlib/waitfreelist.h>
#include "internal/assert.h"
#include <stdio.h>
#include <errno.h>
#include "lithe.h"
#include "futex.h"
#include <time.h>
#include <limits.h>
#include <stdint.h>
#include <stdlib.h>
#include <parlib/reactor.h>

struct futex_list;

/* TIMED WAITS (2026-10-20): futex_wait_timeout parks with a callback timer on
 * parlib's reactor queue. A wake and the timeout race through `state` (CAS from
 * FX_WAITING): the winner unblocks the context, the loser does nothing. A
 * waker that loses moves on to the next waiter so a wake is never spent on a
 * waiter that already timed out. The timer is armed inside the list lock in
 * the block callback, so no waker can resume the context before it exists;
 * the resumed context cancels it (parlib_timer_cancel waits out a callback in
 * flight) before its stack element goes away. */
enum { FX_WAITING = 0, FX_WOKEN = 1, FX_TIMEDOUT = 2 };
struct futex_element {
  STAILQ_ENTRY(futex_element) next;
  lithe_context_t *context;
  struct futex_list *list;
  int val;
  int state;
  int on_list;
  uint64_t deadline_ns;   /* 0 = untimed */
  struct parlib_timer timer;
};

STAILQ_HEAD(futex_tailq, futex_element);

struct futex_list {
  struct futex_tailq tailq;
  int *uaddr;
  spinlock_t lock;
  struct futex_list *hnext;   /* hash chain (FUTEX_HASH) */
};

/* FUTEX HASH (2026-10-24, GAPS B57): queues were found by a linear walk of ONE
 * insert-only list of every address ever waited on, so a runtime that futex-waits
 * on per-allocation words grows every lookup without bound. The OpenMP de-spin lane
 * hit it: libgomp doacross with one futex word per entry under schedule(dynamic,1)
 * spent 62 % of cycles in get_futex_list, 6x slower. Now: 4096 buckets keyed by
 * address, each an insert-only chain read without a lock (publish with release
 * CAS, read with acquire). Queues are still never freed (their addresses are
 * reused). LITHE_FUTEX_LINEAR=1 restores the single list for A/B. */
#define FUTEX_HASH_BITS 12
static struct futex_list *futex_hash[1u << FUTEX_HASH_BITS];
static inline unsigned futex_hash_of(int *uaddr)
{
  uintptr_t a = (uintptr_t)uaddr >> 2;
  return (unsigned)((a * 0x9E3779B97F4A7C15ull) >> (64 - FUTEX_HASH_BITS));
}
static int futex_linear_on(void)
{
  static int on = -1;
  if (__builtin_expect(on < 0, 0)) {
    const char *e = getenv("LITHE_FUTEX_LINEAR");
    on = (e && *e) ? (atoi(e) != 0) : 0;
  }
  return on;
}

/* A list of futex blocking queues, one per uaddr. */
/* LITHE_FUTEX_WAKE_PEEK=1 (2026-10-26, default off: NULL at n=13, GAPS B67).
 * Every wake looked up -- and on a new address created, under the global
 * futex_lists_lock, with a malloc never freed -- the address's list, then took its
 * spinlock, even with nobody waiting. Hosted MPI wakes the waiter's (stack) sync on
 * every completion, and libgomp wakes on every barrier release. Peek mode: a wake
 * only looks lists up (no list = no waiter) and returns without the lock when the
 * queue is empty. Safe because the waiter now enqueues BEFORE checking *uaddr, with
 * a full fence between, and the waker fences between its store to *uaddr (made by
 * the caller) and the peek: one of the two sees the other (Dekker). */
static int futex_peek_on(void)
{
  static int on = -1;
  if (__builtin_expect(on < 0, 0)) {
    const char *e = getenv("LITHE_FUTEX_WAKE_PEEK");
    on = (e && e[0] == '1') ? 1 : 0;
  }
  return on;
}

static struct wfl futex_lists = WFL_INITIALIZER(futex_lists);
static spinlock_t futex_lists_lock = SPINLOCK_INITIALIZER;

/* Find or create the blocking queue that corresponds to the uaddr. */
static struct futex_list *get_futex_list(int *uaddr)
{
  struct futex_list *list;
  if (!futex_linear_on()) {
    struct futex_list **b = &futex_hash[futex_hash_of(uaddr)];
    for (list = __atomic_load_n(b, __ATOMIC_ACQUIRE); list; list = list->hnext)
      if (list->uaddr == uaddr)
        return list;
    spinlock_lock(&futex_lists_lock);
      for (list = __atomic_load_n(b, __ATOMIC_ACQUIRE); list; list = list->hnext)
        if (list->uaddr == uaddr)
          break;
      if (list == NULL) {
        list = malloc(sizeof(struct futex_list));
        if (list == NULL)
          abort();
        STAILQ_INIT(&list->tailq);
        list->uaddr = uaddr;
        spinlock_init(&list->lock);
        list->hnext = *b;
        __atomic_store_n(b, list, __ATOMIC_RELEASE);
        /* not on the wfl: wfl_insert scans for a free slot, O(n) per new queue */
      }
    spinlock_unlock(&futex_lists_lock);
    return list;
  }
  wfl_foreach_unsafe(list, &futex_lists) {
    if (list->uaddr == uaddr)
      return list;
  }

  spinlock_lock(&futex_lists_lock);
    wfl_foreach_unsafe(list, &futex_lists) {
      if (list->uaddr == uaddr)
        break;
    }
    if (list == NULL) {
      list = malloc(sizeof(struct futex_list));
      if (list == NULL)
        abort();
      STAILQ_INIT(&list->tailq);
      list->uaddr = uaddr;
      spinlock_init(&list->lock);
      list->hnext = NULL;
      wfl_insert(&futex_lists, list);
    }
  spinlock_unlock(&futex_lists_lock);

  return list;
}

/* fork() child (lithe.c lithe_fork_child): every waiter on every futex queue
 * is a parent-process context (stale; it never runs here), an element may be
 * half-built by a hart that was mid-wait at fork time, and a queue lock may be
 * held by a hart that does not exist here. Empty the queues, reset the locks.
 * The queue objects themselves stay (keyed by address, reused). */
void lithe_futex_after_fork(void)
{
  struct futex_list *list;
  spinlock_init(&futex_lists_lock);
  wfl_foreach_unsafe(list, &futex_lists) {
    STAILQ_INIT(&list->tailq);
    spinlock_init(&list->lock);
  }
  for (unsigned i = 0; i < (1u << FUTEX_HASH_BITS); i++)
    for (list = futex_hash[i]; list; list = list->hnext) {
      STAILQ_INIT(&list->tailq);
      spinlock_init(&list->lock);
    }
}

static void __futex_timeout(void *arg)
{
  struct futex_element *e = arg;
  int exp = FX_WAITING;
  if (!__atomic_compare_exchange_n(&e->state, &exp, FX_TIMEDOUT, false,
                                   __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE))
    return;   /* a waker won */
  spinlock_lock(&e->list->lock);
    if (e->on_list) {
      STAILQ_REMOVE(&e->list->tailq, e, futex_element, next);
      e->on_list = 0;
    }
  spinlock_unlock(&e->list->lock);
  lithe_context_unblock(e->context);
}

/* Claim e for a waker: WAITING -> WOKEN. Called UNDER e->list->lock. A waiter whose
 * timeout won (TIMEDOUT) is dropped: its timer callback takes this lock after us and
 * unblocks it, and the waker never touches e again once the lock is released, because
 * that waiter may resume and reuse its stack element (2026-10-20: claiming after the
 * unlock read a reused element and double-unblocked, corrupting the list: tf.c (d)). */
static inline int __futex_claim_locked(struct futex_element *e)
{
  int exp = FX_WAITING;
  e->on_list = 0;
  return __atomic_compare_exchange_n(&e->state, &exp, FX_WOKEN, false,
                                     __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE);
}

/* Peek mode: the list for uaddr if one exists, NULL otherwise (no allocation). */
static struct futex_list *find_futex_list(int *uaddr)
{
  struct futex_list *list;
  if (futex_linear_on())
    return get_futex_list(uaddr);
  for (list = __atomic_load_n(&futex_hash[futex_hash_of(uaddr)], __ATOMIC_ACQUIRE);
       list; list = list->hnext)
    if (list->uaddr == uaddr)
      return list;
  return NULL;
}

/* Peek mode: NULL if no waiter can be queued on uaddr, else its list. */
static inline struct futex_list *futex_wake_list(int *uaddr)
{
  if (!futex_peek_on())
    return get_futex_list(uaddr);
  __atomic_thread_fence(__ATOMIC_SEQ_CST); /* the caller's store to *uaddr before the peek */
  struct futex_list *list = find_futex_list(uaddr);
  if (list == NULL ||
      __atomic_load_n(&list->tailq.stqh_first, __ATOMIC_ACQUIRE) == NULL)
    return NULL;
  return list;
}

/* lithe_context_block callback.  Atomically checks uaddr == val and blocks. */
static void __futex_block(lithe_context_t *context, void *arg) {
  struct futex_element *e = arg;
  bool block = true;

  if (futex_peek_on()) {
    /* Enqueue, fence, then check (pairs with futex_wake_list). */
    spinlock_lock(&e->list->lock);
      e->context = context;
      STAILQ_INSERT_TAIL(&e->list->tailq, e, next);
      e->on_list = 1;
      __atomic_thread_fence(__ATOMIC_SEQ_CST);
      if (*(volatile int *)e->list->uaddr == e->val) {
        if (e->deadline_ns &&
            parlib_timer_start(&e->timer, e->deadline_ns, __futex_timeout, e) < 0)
          e->deadline_ns = 0;
      } else {
        /* still ours: wakers claim only under this lock */
        STAILQ_REMOVE(&e->list->tailq, e, futex_element, next);
        e->on_list = 0;
        block = false;
        e->deadline_ns = 0;
      }
    spinlock_unlock(&e->list->lock);
    if (!block)
      lithe_context_unblock(context);
    return;
  }

  spinlock_lock(&e->list->lock);
    if (*e->list->uaddr == e->val) {
      e->context = context;
      STAILQ_INSERT_TAIL(&e->list->tailq, e, next);
      e->on_list = 1;
      if (e->deadline_ns &&
          parlib_timer_start(&e->timer, e->deadline_ns, __futex_timeout, e) < 0)
        e->deadline_ns = 0;   /* no reactor: untimed */
    } else {
      block = false;
      e->deadline_ns = 0;   /* no timer was started: the caller must not cancel one */
    }
  spinlock_unlock(&e->list->lock);

  if (!block)
    lithe_context_unblock(context);
}

static int __futex_wait_until(int *uaddr, int val, uint64_t deadline_ns)
{
  if (*uaddr != val)
    return 0;
  struct futex_element e;
  e.list = get_futex_list(uaddr);
  e.val = val;
  e.state = FX_WAITING;
  e.on_list = 0;
  e.context = NULL;
  e.deadline_ns = deadline_ns;
  lithe_context_block(__futex_block, &e);
  if (e.deadline_ns)
    (void)parlib_timer_cancel(&e.timer);
  if (__atomic_load_n(&e.state, __ATOMIC_ACQUIRE) == FX_TIMEDOUT) {
    errno = ETIMEDOUT;
    return -1;
  }
  return 0;
}

int futex_wait(int *uaddr, int val)
{
  return __futex_wait_until(uaddr, val, 0);
}

int futex_wait_timeout(int *uaddr, int val, const struct timespec *rel)
{
  if (rel == NULL)
    return futex_wait(uaddr, val);
  if (rel->tv_sec < 0 || (rel->tv_sec == 0 && rel->tv_nsec <= 0)) {
    if (*uaddr != val)
      return 0;
    errno = ETIMEDOUT;
    return -1;
  }
  struct timespec now;
  clock_gettime(CLOCK_MONOTONIC, &now);
  uint64_t d = (uint64_t)now.tv_sec * 1000000000ull + (uint64_t)now.tv_nsec
             + (uint64_t)rel->tv_sec * 1000000000ull + (uint64_t)rel->tv_nsec;
  return __futex_wait_until(uaddr, val, d);
}

int futex_wake_one(int *uaddr)
{
  struct futex_list *list = futex_wake_list(uaddr);
  struct futex_element *e;
  if (list == NULL)
    return 0;
  spinlock_lock(&list->lock);
    while ((e = STAILQ_FIRST(&list->tailq)) != NULL) {
      STAILQ_REMOVE_HEAD(&list->tailq, next);
      if (__futex_claim_locked(e))
        break;
      /* it timed out under us: wake the next one instead */
    }
  spinlock_unlock(&list->lock);
  if (e == NULL)
    return 0;
  lithe_context_unblock(e->context);
  return 1;
}

/* Unblock the claimed waiters on q (they cannot resume before this, so q is valid). */
static inline int unblock_futex_queue(struct futex_tailq *q)
{
  int num = 0;
  struct futex_element *e,*n;
  for (e = STAILQ_FIRST(q); e != NULL; e = n) {
    n = STAILQ_NEXT(e, next);
    lithe_context_unblock(e->context);
    num++;
  }

  return num;
}

int futex_wake_all(int *uaddr)
{
  return futex_wake_some(uaddr, INT_MAX);
}

int futex_wake_some(int *uaddr, int count)
{
  struct futex_tailq q = STAILQ_HEAD_INITIALIZER(q);
  struct futex_list *list = futex_wake_list(uaddr);
  struct futex_element *e;

  if (list == NULL)
    return 0;
  spinlock_lock(&list->lock);
    /* Remove up to count claimable entries from the queue, and remember them locally. */
    while (count > 0 && (e = STAILQ_FIRST(&list->tailq)) != NULL) {
      STAILQ_REMOVE_HEAD(&list->tailq, next);
      if (__futex_claim_locked(e)) {
        STAILQ_INSERT_TAIL(&q, e, next);
        count--;
      }
    }
  spinlock_unlock(&list->lock);

  return unblock_futex_queue(&q);
}

int futex(int *uaddr, int op, int val, const struct timespec *timeout,
                 int *uaddr2, int val3)
{
  assert(uaddr2 == NULL);
  assert(val3 == 0);

  switch(op) {
    case FUTEX_WAIT:
      return futex_wait_timeout(uaddr, val, timeout);   /* relative, as FUTEX_WAIT */
    case FUTEX_WAKE:
      return futex_wake_some(uaddr, val);
    default:
      errno = ENOSYS;
      return -1;
  }
  return -1;
}

