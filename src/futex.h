#ifndef LITHE_FUTEX_H
#define LITHE_FUTEX_H

#include <sys/time.h>

#define FUTEX_WAIT 0
#define FUTEX_WAKE 1

#ifdef __cplusplus
extern "C" {
#endif

/* Traditional futex call, passing flags. */
int futex(int *uaddr, int op, int val, const struct timespec *timeout,
          int *uaddr2, int val3);

/* Simple wait call, that almost everyone usually makes.  Equivalent to 
 * futex(uaddr, FUTEX_WAIT, val, NULL, NULL, 0) */
int futex_wait(int *uaddr, int val);

/* futex_wait with a relative timeout (NULL = forever): 0 when woken or *uaddr !=
 * val, -1 with errno ETIMEDOUT when the timeout passed first. The timeout is a
 * timer on parlib's reactor queue, not a kernel wait: the hart runs other
 * contexts meanwhile. */
int futex_wait_timeout(int *uaddr, int val, const struct timespec *rel);


/* Variants on the wakeup call, optimized for the most common cases of wake
 * one or wake all.*/
int futex_wake_one(int *uaddr);
int futex_wake_all(int *uaddr);
int futex_wake_some(int *uaddr, int count);

/* ---- Parking lot (docs/PLAN-locking.md P1, 2026-10-26) ----------------------------
 * One address-keyed wait table under every Lithe blocking primitive (WebKit ParkingLot /
 * Rust parking_lot). Waiter nodes live on the waiter's stack: no allocation, nothing to
 * free. Must be called from a Lithe context that can block (lithe_can_block()); returns
 * LITHE_PARK_NOTSUPP otherwise and the caller falls back.
 *
 * lithe_park: the waiter is enqueued on addr's bucket, then validate(arg) runs under the
 * bucket lock (enqueue first, full fence, then validate: a waker that changed the
 * condition and then sees an empty bucket can skip the lock). If validate returns 0 the
 * waiter is dequeued and LITHE_PARK_INVALID is returned at once. deadline_ns
 * (CLOCK_MONOTONIC, 0 = none) gives LITHE_PARK_TIMEOUT. *token gets the unparker's token.
 *
 * lithe_unpark_one: wake one waiter on addr. cb(arg, unparked, more, waited_ns), if
 * given, runs under the bucket lock with whether a waiter was woken, whether more remain,
 * and how long the woken one waited; its return value is the woken waiter's token.
 * lithe_unpark_requeue: wake up to nwake waiters on from, move the rest to to; cb(arg,
 * moved) runs under both bucket locks before anything moves and returns 0 to wake all
 * instead of moving. */
enum { LITHE_PARK_UNPARKED = 0, LITHE_PARK_INVALID = 1, LITHE_PARK_TIMEOUT = 2,
       LITHE_PARK_NOTSUPP = 3 };
int lithe_park(const void *addr, int (*validate)(void *), void *arg,
               unsigned long long deadline_ns, long *token);
int lithe_unpark_one(const void *addr,
                     long (*cb)(void *arg, int unparked, int more,
                                unsigned long long waited_ns),
                     void *arg);
int lithe_unpark_all(const void *addr);
int lithe_unpark_requeue(const void *from, const void *to, int nwake,
                         int (*cb)(void *arg, int moving), void *arg);
/* 1 if any waiter may be parked on addr's bucket (lock-free, conservative; the caller
 * must have published its condition change and fenced first). */
int lithe_park_maybe_waiters(const void *addr);
/* Locality of the calling hart: L3 group id (-1 unknown). */
int lithe_park_self_l3(void);

#define futex_wake(uaddr, count) \
( \
  __builtin_constant_p(count) && (count) == 1 ? futex_wake_one(uaddr) : \
  __builtin_constant_p(count) && (count) == INT_MAX ? futex_wake_all(uaddr) : \
  futex_wake_some(uaddr, count) \
)

#ifdef __cplusplus
}
#endif

#endif	/* LITHE_FUTEX_H */
