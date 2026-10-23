/* glibc futex hook (lithe-docker install-glibc-lithe, 2026-10-23; LITHE_LIBC_FUTEX=1, default
 * off). The Lithe glibc sends every futex operation of its own locks -- the lowlevellock
 * waits behind malloc's arena locks, stdio, the loader lock, pthread mutexes, condvars,
 * once, semaphores, barriers -- through __lithe_futex_hook. Here:
 *   - a private WAIT by a context that may block parks the context (lithe_context_block)
 *     instead of the hart, with a timeout timer on parlib's reactor;
 *   - a private WAKE wakes such waiters, then the kernel's for what is left (a waiter that
 *     is not a context, or one that could not block cooperatively, sleeps in the kernel);
 *   - everything else (shared futexes: process-shared objects and the kernel's tid clear;
 *     requeue, PI and wake-op) is the syscall.
 * No malloc anywhere (this runs inside malloc's own lock waits): waiters sit on the
 * caller's stack in fixed hash buckets. Return values are the kernel's: 0 / -errno, or the
 * number woken. */
#include <errno.h>
#include <limits.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/queue.h>
#include <sys/syscall.h>
#include <linux/futex.h>
#include <cpuid.h>
#include <parlib/reactor.h>
#include <parlib/spinlock.h>
#include "lithe.h"

#define NBUCKETS 512

enum { W_WAITING = 0, W_WOKEN = 1, W_TIMEDOUT = 2 };
struct lwaiter {
  TAILQ_ENTRY(lwaiter) link;
  int *uaddr;
  int val;
  int state;
  int on_list;
  uint64_t deadline_ns;   /* CLOCK_MONOTONIC; 0 = untimed */
  lithe_context_t *context;
  struct lbucket *b;
  struct parlib_timer timer;
};
TAILQ_HEAD(lwq, lwaiter);
struct lbucket {
  spinlock_t lock;
  int n;                  /* waiters on q: a wake skips empty buckets without the lock */
  struct lwq q;
} __attribute__((aligned(64)));
static struct lbucket buckets[NBUCKETS];

static inline struct lbucket *bucket_of(int *uaddr)
{
  uintptr_t a = (uintptr_t)uaddr;
  return &buckets[((a >> 2) ^ (a >> 11) ^ (a >> 20)) & (NBUCKETS - 1)];
}

static inline long raw_futex(int *uaddr, int op, int val, const struct timespec *ts,
                             int *uaddr2, int val3)
{
  long r;
  register long r10 __asm__("r10") = (long)ts;
  register long r8 __asm__("r8") = (long)uaddr2;
  register long r9 __asm__("r9") = val3;
  __asm__ volatile("syscall"
                   : "=a"(r)
                   : "0"((long)SYS_futex), "D"(uaddr), "S"(op), "d"(val), "r"(r10), "r"(r8),
                     "r"(r9)
                   : "rcx", "r11", "memory");
  return r;
}

static uint64_t mono_ns(void)
{
  struct timespec t;
  clock_gettime(CLOCK_MONOTONIC, &t);
  return (uint64_t)t.tv_sec * 1000000000ull + (uint64_t)t.tv_nsec;
}

static void lw_timeout(void *arg)
{
  struct lwaiter *w = arg;
  int exp = W_WAITING;
  if (!__atomic_compare_exchange_n(&w->state, &exp, W_TIMEDOUT, 0, __ATOMIC_ACQ_REL,
                                   __ATOMIC_ACQUIRE))
    return;   /* a waker won */
  spinlock_lock(&w->b->lock);
  if (w->on_list) {
    TAILQ_REMOVE(&w->b->q, w, link);
    w->on_list = 0;
    __atomic_sub_fetch(&w->b->n, 1, __ATOMIC_SEQ_CST);
  }
  spinlock_unlock(&w->b->lock);
  lithe_context_unblock(w->context);
}

/* lithe_context_block callback (vcore context): queue unless the word already changed. */
static void lw_block(lithe_context_t *ctx, void *arg)
{
  struct lwaiter *w = arg;
  int queued = 0;
  spinlock_lock(&w->b->lock);
  /* Publish in n BEFORE reading the word (a full fence between): a waker stores the word,
   * then reads n locklessly to skip empty buckets. Checking the word first lost wakeups:
   * waker stored + saw n == 0 between our check and our increment (lockmix hung 2/3). */
  __atomic_add_fetch(&w->b->n, 1, __ATOMIC_SEQ_CST);
  if (__atomic_load_n(w->uaddr, __ATOMIC_SEQ_CST) == w->val) {
    w->context = ctx;
    TAILQ_INSERT_TAIL(&w->b->q, w, link);
    w->on_list = 1;
    queued = 1;
    if (w->deadline_ns && parlib_timer_start(&w->timer, w->deadline_ns, lw_timeout, w) < 0)
      w->deadline_ns = 0;
  } else {
    __atomic_sub_fetch(&w->b->n, 1, __ATOMIC_SEQ_CST);
    w->state = W_WOKEN;     /* not queued: report EAGAIN */
    w->deadline_ns = 0;
  }
  spinlock_unlock(&w->b->lock);
  if (!queued)
    lithe_context_unblock(ctx);
}

static long lw_wait(int *uaddr, int val, uint64_t deadline_ns)
{
  if (__atomic_load_n(uaddr, __ATOMIC_SEQ_CST) != val)
    return -EAGAIN;
  if (deadline_ns && mono_ns() >= deadline_ns)
    return -ETIMEDOUT;
  struct lwaiter w;
  memset(&w, 0, sizeof(w));
  w.uaddr = uaddr;
  w.val = val;
  w.state = W_WAITING;
  w.deadline_ns = deadline_ns;
  w.b = bucket_of(uaddr);
  lithe_context_block(lw_block, &w);
  if (w.deadline_ns)
    (void)parlib_timer_cancel(&w.timer);
  int st = __atomic_load_n(&w.state, __ATOMIC_ACQUIRE);
  return st == W_TIMEDOUT ? -ETIMEDOUT : 0;
}

static int lw_wake(int *uaddr, int count)
{
  struct lbucket *b = bucket_of(uaddr);
  if (__atomic_load_n(&b->n, __ATOMIC_SEQ_CST) == 0)
    return 0;
  struct lwq woken = TAILQ_HEAD_INITIALIZER(woken);
  int n = 0;
  spinlock_lock(&b->lock);
  struct lwaiter *w, *nx;
  for (w = TAILQ_FIRST(&b->q); w != NULL && n < count; w = nx) {
    nx = TAILQ_NEXT(w, link);
    if (w->uaddr != uaddr)
      continue;
    TAILQ_REMOVE(&b->q, w, link);
    w->on_list = 0;
    __atomic_sub_fetch(&b->n, 1, __ATOMIC_SEQ_CST);
    int exp = W_WAITING;
    /* claimed under the lock: a waiter whose timeout won is left to its timer */
    if (__atomic_compare_exchange_n(&w->state, &exp, W_WOKEN, 0, __ATOMIC_ACQ_REL,
                                    __ATOMIC_ACQUIRE)) {
      TAILQ_INSERT_TAIL(&woken, w, link);
      n++;
    }
  }
  spinlock_unlock(&b->lock);
  for (w = TAILQ_FIRST(&woken); w != NULL; w = nx) {
    nx = TAILQ_NEXT(w, link);   /* read before the unblock: w may be gone after it */
    lithe_context_unblock(w->context);
  }
  return n;
}

/* deadline from a FUTEX_WAIT (relative) or FUTEX_WAIT_BITSET (absolute, either clock) */
static uint64_t deadline_of(int cmd, int op, const struct timespec *ts)
{
  if (ts == NULL)
    return 0;
  uint64_t t = (uint64_t)ts->tv_sec * 1000000000ull + (uint64_t)ts->tv_nsec;
  if (cmd == FUTEX_WAIT)
    return mono_ns() + t;
  if (op & FUTEX_CLOCK_REALTIME) {
    struct timespec now;
    clock_gettime(CLOCK_REALTIME, &now);
    uint64_t rn = (uint64_t)now.tv_sec * 1000000000ull + (uint64_t)now.tv_nsec;
    uint64_t m = mono_ns();
    return t > rn ? m + (t - rn) : m;
  }
  return t ? t : 1;
}

static long lithe_libc_futex(int *uaddr, int op, int val, const struct timespec *ts,
                             int *uaddr2, int val3)
{
  int cmd = op & FUTEX_CMD_MASK;
  if (!(op & FUTEX_PRIVATE_FLAG))
    return raw_futex(uaddr, op, val, ts, uaddr2, val3);
  if ((cmd == FUTEX_WAIT || (cmd == FUTEX_WAIT_BITSET && val3 == (int)FUTEX_BITSET_MATCH_ANY))
      && lithe_can_block())
    return lw_wait(uaddr, val, deadline_of(cmd, op, ts));
  if (cmd == FUTEX_WAKE || (cmd == FUTEX_WAKE_BITSET && val3 == (int)FUTEX_BITSET_MATCH_ANY)) {
    int n = lw_wake(uaddr, val);
    if (n >= val)
      return n;
    long k = raw_futex(uaddr, op, val - n, NULL, NULL, val3);
    return k < 0 ? (n > 0 ? n : k) : n + k;
  }
  return raw_futex(uaddr, op, val, ts, uaddr2, val3);
}

struct lithe_libc_hooks {
  size_t size;
  long (*futex)(int *, int, int, const struct timespec *, int *, int);
  int (*sig_dispatch)(int, void *, void *);
  void (*sigmask_changed)(void);
};
extern void *parlib_libc_symbol(const char *name);

/* lithe.c, at library init. */
void lithe_libc_futex_init(void)
{
  const char *e = getenv("LITHE_LIBC_FUTEX");
  if (!(e && e[0] == '1'))
    return;
  for (int i = 0; i < NBUCKETS; i++) {
    spinlock_init(&buckets[i].lock);
    TAILQ_INIT(&buckets[i].q);
  }
  unsigned a, b, c, d;
  __cpuid_count(0xd, 0, a, b, c, d);
  if (b == 0 || b > 4096)   /* glibc's trampoline saves the vector state in 4096 bytes */
    return;
  int (*set)(const struct lithe_libc_hooks *) = parlib_libc_symbol("__lithe_libc_set_hooks");
  if (set == NULL)
    return;
  struct lithe_libc_hooks h = { sizeof(h), lithe_libc_futex, NULL, NULL };
  set(&h);
}

/* fork child: waiters were parent contexts. */
void lithe_libc_futex_after_fork(void)
{
  for (int i = 0; i < NBUCKETS; i++) {
    spinlock_init(&buckets[i].lock);
    TAILQ_INIT(&buckets[i].q);
    buckets[i].n = 0;
  }
}
