#include <sys/queue.h>
#include <parlib/waitfreelist.h>
#include "internal/assert.h"
#include <stdio.h>
#include <errno.h>
#include "lithe.h"
#include "futex.h"
#include <time.h>
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
};

/* A list of futex blocking queues, one per uaddr. */
static struct wfl futex_lists = WFL_INITIALIZER(futex_lists);
static spinlock_t futex_lists_lock = SPINLOCK_INITIALIZER;

/* Find or create the blocking queue that corresponds to the uaddr. */
static struct futex_list *get_futex_list(int *uaddr)
{
  struct futex_list *list;
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
      wfl_insert(&futex_lists, list);
    }
  spinlock_unlock(&futex_lists_lock);

  return list;
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

/* A waker took e off the list: unblock it unless it already timed out. */
static inline int __futex_claim(struct futex_element *e)
{
  int exp = FX_WAITING;
  if (!__atomic_compare_exchange_n(&e->state, &exp, FX_WOKEN, false,
                                   __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE))
    return 0;
  lithe_context_unblock(e->context);
  return 1;
}

/* lithe_context_block callback.  Atomically checks uaddr == val and blocks. */
static void __futex_block(lithe_context_t *context, void *arg) {
  struct futex_element *e = arg;
  bool block = true;

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
  struct futex_list *list = get_futex_list(uaddr);
  for (;;) {
    spinlock_lock(&list->lock);
      struct futex_element *e = STAILQ_FIRST(&list->tailq);
      if (e != NULL) {
        STAILQ_REMOVE_HEAD(&list->tailq, next);
        e->on_list = 0;
      }
    spinlock_unlock(&list->lock);
    if (e == NULL)
      return 0;
    if (__futex_claim(e))
      return 1;
    /* it timed out under us: wake the next one instead */
  }
}

static inline int unblock_futex_queue(struct futex_tailq *q)
{
  int num = 0;
  struct futex_element *e,*n;
  for (e = STAILQ_FIRST(q); e != NULL; e = n) {
    n = STAILQ_NEXT(e, next);
    num += __futex_claim(e);
  }

  return num;
}

int futex_wake_all(int *uaddr)
{
  struct futex_list *list = get_futex_list(uaddr);
  struct futex_element *e;

  spinlock_lock(&list->lock);
    STAILQ_FOREACH(e, &list->tailq, next)
      e->on_list = 0;
    struct futex_tailq q = list->tailq;
    STAILQ_INIT(&list->tailq);
  spinlock_unlock(&list->lock);

  return unblock_futex_queue(&q);
}

int futex_wake_some(int *uaddr, int count)
{
  struct futex_tailq q = STAILQ_HEAD_INITIALIZER(q);
  struct futex_list *list = get_futex_list(uaddr);
  struct futex_element *e;

  spinlock_lock(&list->lock);
    /* Remove up to count entries from the queue, and remeber them locally. */
    while (count-- > 0 && (e = STAILQ_FIRST(&list->tailq)) != NULL) {
      STAILQ_REMOVE_HEAD(&list->tailq, next);
      e->on_list = 0;
      STAILQ_INSERT_TAIL(&q, e, next);
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

