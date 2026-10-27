#define _GNU_SOURCE
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


/* ====================== Lock profiler (docs/PLAN-locking.md P0) ======================
 * LITHE_LOCK_PROF=1: per call site (return address of lithe_mutex_lock/trylock),
 * count acquisitions, contended acquisitions, parks, contentions where the owner was
 * not on a hart, and wait time. Per-hart tables (no shared lines on the hot path);
 * merged and printed to stderr (or LITHE_LOCK_PROF_FILE.<pid>) at exit, top 25 by
 * wait. Symbolize with dladdr; for static binaries the raw pc goes to addr2line. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <dlfcn.h>
#include "futex.h"
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <parlib/vcore.h>
#include <parlib/uthread.h>

#define LPROF_SLOTS 512
struct lprof_ent {
  void *pc;
  unsigned long acq, cont, parked, owner_off, try_fail;
  unsigned long long wait_ns, max_wait_ns;
};
static struct lprof_ent *lprof_tab;   /* [max_vcores()+1][LPROF_SLOTS] */
static int lprof_nv, lprof_on = -1;

static inline uint64_t lk_now_ns(void)
{
  struct timespec t;
  clock_gettime(CLOCK_MONOTONIC, &t);
  return (uint64_t)t.tv_sec * 1000000000ull + (uint64_t)t.tv_nsec;
}

static void lprof_dump(void);
static int lprof_enabled(void)
{
  if (__builtin_expect(lprof_on < 0, 0)) {
    const char *e = getenv("LITHE_LOCK_PROF");
    int on = (e && e[0] == '1');
    if (on) {
      lprof_nv = max_vcores() + 1;
      lprof_tab = calloc((size_t)lprof_nv * LPROF_SLOTS, sizeof(struct lprof_ent));
      if (!lprof_tab)
        on = 0;
      else
        atexit(lprof_dump);
    }
    __atomic_store_n(&lprof_on, on, __ATOMIC_RELEASE);
  }
  return lprof_on;
}

static inline struct lprof_ent *lprof_slot(void *pc)
{
  int v = (current_uthread && !in_vcore_context()) ? vcore_id() : lprof_nv - 1;
  if (v < 0 || v >= lprof_nv)
    v = lprof_nv - 1;   /* the last row is shared by non-hart callers (racy, fine) */
  struct lprof_ent *t = lprof_tab + (size_t)v * LPROF_SLOTS;
  unsigned h = (unsigned)(((uintptr_t)pc >> 3) * 0x9E3779B1u) % LPROF_SLOTS;
  for (int i = 0; i < 16; i++) {
    struct lprof_ent *e = &t[(h + i) % LPROF_SLOTS];
    if (e->pc == pc)
      return e;
    if (e->pc == NULL) {
      e->pc = pc;
      return e;
    }
  }
  return NULL;
}

static void lprof_record(void *pc, int contended, int parked, int owner_off,
                         uint64_t wait_ns)
{
  struct lprof_ent *e = lprof_slot(pc);
  if (!e)
    return;
  e->acq++;
  if (contended) {
    e->cont++;
    e->parked += parked;
    e->owner_off += owner_off;
    e->wait_ns += wait_ns;
    if (wait_ns > e->max_wait_ns)
      e->max_wait_ns = wait_ns;
  }
}

static int lprof_cmp(const void *a, const void *b)
{
  const struct lprof_ent *x = a, *y = b;
  if (x->wait_ns != y->wait_ns)
    return x->wait_ns < y->wait_ns ? 1 : -1;
  return x->acq < y->acq ? 1 : (x->acq > y->acq ? -1 : 0);
}

static void lprof_dump(void)
{
  struct lprof_ent *m = calloc(LPROF_SLOTS * 4, sizeof(*m));
  int n = 0;
  if (!m)
    return;
  for (int v = 0; v < lprof_nv; v++)
    for (int i = 0; i < LPROF_SLOTS; i++) {
      struct lprof_ent *e = &lprof_tab[(size_t)v * LPROF_SLOTS + i];
      if (!e->pc)
        continue;
      int j;
      for (j = 0; j < n && m[j].pc != e->pc; j++)
        ;
      if (j == n) {
        if (n == LPROF_SLOTS * 4)
          continue;
        m[n++].pc = e->pc;
      }
      m[j].acq += e->acq; m[j].cont += e->cont; m[j].parked += e->parked;
      m[j].owner_off += e->owner_off; m[j].try_fail += e->try_fail;
      m[j].wait_ns += e->wait_ns;
      if (e->max_wait_ns > m[j].max_wait_ns)
        m[j].max_wait_ns = e->max_wait_ns;
    }
  qsort(m, n, sizeof(*m), lprof_cmp);
  FILE *f = stderr;
  const char *path = getenv("LITHE_LOCK_PROF_FILE");
  char buf[512];
  if (path) {
    snprintf(buf, sizeof(buf), "%s.%d", path, (int)getpid());
    FILE *g = fopen(buf, "w");
    if (g)
      f = g;
  }
  unsigned long ta = 0, tc = 0, tp = 0, to = 0;
  unsigned long long tw = 0;
  for (int j = 0; j < n; j++) {
    ta += m[j].acq; tc += m[j].cont; tp += m[j].parked; to += m[j].owner_off; tw += m[j].wait_ns;
  }
  fprintf(f, "[lock_prof] pid %d sites %d acq %lu contended %lu (%.2f%%) parked %lu "
          "owner_off %lu wait_ms %.3f\n", (int)getpid(), n, ta, tc,
          ta ? 100.0 * tc / ta : 0.0, tp, to, tw / 1e6);
  fprintf(f, "[lock_prof] %-18s %10s %9s %8s %8s %9s %10s %9s  %s\n", "pc", "acq", "cont",
          "parked", "own_off", "tryfail", "wait_ms", "max_us", "symbol");
  for (int j = 0; j < n && j < 25; j++) {
    Dl_info di;
    const char *sym = "?";
    long off = 0;
    char fb[512];
    if (dladdr(m[j].pc, &di)) {
      if (di.dli_sname) {
        sym = di.dli_sname;
        off = (long)((char *)m[j].pc - (char *)di.dli_saddr);
      } else if (di.dli_fname && di.dli_fbase) {
        /* static symbol in a shared object: file + offset for addr2line -e */
        const char *b = strrchr(di.dli_fname, '/');
        snprintf(fb, sizeof fb, "%s@0x%lx", di.dli_fname,
                 (unsigned long)((char *)m[j].pc - (char *)di.dli_fbase));
        sym = fb;
        (void)b;
      }
    }
    fprintf(f, "[lock_prof] %-18p %10lu %9lu %8lu %8lu %9lu %10.3f %9.1f  %s+%ld\n",
            m[j].pc, m[j].acq, m[j].cont, m[j].parked, m[j].owner_off, m[j].try_fail,
            m[j].wait_ns / 1e6, m[j].max_wait_ns / 1e3, sym, off);
  }
  if (f != stderr)
    fclose(f);
  free(m);
}

/* ====================== Parking-lot mutex (docs/PLAN-locking.md P2) ==================
 * LITHE_MUTEX_LOT=1 (2026-10-26, default off for A/B; non-recursive mutexes; takes
 * precedence over LITHE_MUTEX_FAST). `locked` is a word with LOCKED=1, PARKED=2; the
 * waiters live in the parking lot keyed by &locked, so the mutex's own queue and MCS lock
 * are unused. `owner` is a hint (relaxed) and `qnode` carries the owner's vcore + 1, so
 * the struct layout (embedded by libfabric, OMPI) is unchanged.
 *
 * Contended lock: spin only while the owner is on a hart (uth.state == UT_RUNNING) and for
 * at most LITHE_LOCK_SPIN_NS (default 1500 ns, about one same-L3 wake, B62); with
 * LITHE_LOCK_SPIN_TOPO=1 the budget is scaled by the owner's distance (x3 another L3,
 * x6 another NUMA node: B62 wake costs 1.3 / 4.7 / 9.8 us). Otherwise set PARKED and park.
 * A holder that is not running never has spinners.
 *
 * Unlock: CAS LOCKED -> 0; if PARKED, unpark one. If the woken waiter has waited longer
 * than LITHE_LOT_FAIR_NS (1 ms), ownership is handed to it (LOCKED stays set) so a hot
 * lock cannot starve it; otherwise the word becomes PARKED-if-more and the waiter races.
 */
#define LK_LOCKED 1
#define LK_PARKED 2
#define LK_HANDOFF 1L
static int lithe_mutex_lot = -1;
static uint64_t lk_spin_ns = 1500, lk_fair_ns = 1000000;
static int lk_spin_topo = 0;
/* LITHE_LOCK_RQ_AWARE=1 (default 0 until A/B'd): spinning is free only when this
 * hart has nothing else to run. If our fork-join scheduler has runnable contexts
 * without a hart, park at once even if the holder runs -- the hart goes to them.
 * LITHE_LOCK_OFF_SPIN_NS=n (default 0): when the holder is off its hart, still spin
 * up to n ns before parking (a holder that only yielded is often back sooner than a
 * park+wake). */
static int lk_rq_aware = 0;
static uint64_t lk_off_spin_ns = 0;
extern long lithe_fork_join_current_hartless_runnable(void);
static inline int mutex_lot(const lithe_mutex_t *mutex)
{
  if (__builtin_expect(lithe_mutex_lot < 0, 0)) {
    const char *e = getenv("LITHE_MUTEX_LOT");
    const char *sp = getenv("LITHE_LOCK_SPIN_NS");
    const char *fa = getenv("LITHE_LOT_FAIR_NS");
    const char *tp = getenv("LITHE_LOCK_SPIN_TOPO");
    if (sp && atol(sp) >= 0) lk_spin_ns = (uint64_t)atol(sp);
    if (fa && atol(fa) > 0) lk_fair_ns = (uint64_t)atol(fa);
    lk_spin_topo = (tp && tp[0] == '1');
    const char *rq = getenv("LITHE_LOCK_RQ_AWARE");
    lk_rq_aware = (rq && rq[0] == '1');
    const char *os = getenv("LITHE_LOCK_OFF_SPIN_NS");
    if (os && atol(os) > 0) lk_off_spin_ns = (uint64_t)atol(os);
    lithe_mutex_lot = (e && e[0] == '1') ? 1 : 0;
  }
  return lithe_mutex_lot && mutex->attr.type != LITHE_MUTEX_RECURSIVE;
}

static inline void lk_set_owner(lithe_mutex_t *mutex)
{
  __atomic_store_n(&mutex->owner, lithe_context_self(), __ATOMIC_RELAXED);
  if (lk_spin_topo)
    __atomic_store_n(&mutex->qnode,
                     (mcs_lock_qnode_t *)(intptr_t)((current_uthread ? vcore_id() : -1) + 1),
                     __ATOMIC_RELAXED);
}

static int lk_validate(void *arg)
{
  lithe_mutex_t *m = arg;
  return __atomic_load_n(&m->locked, __ATOMIC_ACQUIRE) == (LK_LOCKED | LK_PARKED);
}

static uint64_t lk_budget(lithe_mutex_t *mutex)
{
  if (!lk_spin_topo)
    return lk_spin_ns;
  int ov = (int)(intptr_t)__atomic_load_n(&mutex->qnode, __ATOMIC_RELAXED) - 1;
  int me = current_uthread ? vcore_id() : -1;
  if (ov < 0 || me < 0)
    return lk_spin_ns;
  if (parlib_vcore_l3_group(ov) == parlib_vcore_l3_group(me))
    return lk_spin_ns;
  if (parlib_vcore_numa_node(ov) == parlib_vcore_numa_node(me))
    return lk_spin_ns * 3;
  return lk_spin_ns * 6;
}

/* returns 1 if it parked at least once, for the profiler; *owner_off likewise */
static int lot_lock_slow(lithe_mutex_t *mutex, int *owner_off)
{
  uint64_t t0 = 0, budget = 0;
  int parked = 0;
  unsigned i = 0;
  for (;;) {
    int s = __atomic_load_n(&mutex->locked, __ATOMIC_RELAXED);
    if (!(s & LK_LOCKED)) {
      if (__atomic_compare_exchange_n(&mutex->locked, &s, s | LK_LOCKED, true,
                                      __ATOMIC_ACQUIRE, __ATOMIC_RELAXED))
        break;
      continue;
    }
    {
      /* The owner hint is NULL between a holder's CAS and its store, and after unlock:
       * unknown counts as running (spin within budget), only a known off-hart holder
       * parks at once. Spin even when others are parked (PARKED set): parking at once
       * behind a parked waiter convoyed -- every acquisition contended, the lock idle
       * while each woken waiter waited for a hart (lock_bench holder: 97 % contended
       * vs 12 % with the B67 spin; 2026-10-26). */
      lithe_context_t *o = __atomic_load_n(&mutex->owner, __ATOMIC_RELAXED);
      int running = !o || __atomic_load_n(&o->uth.state, __ATOMIC_RELAXED) == UT_RUNNING;
      if (!running && owner_off)
        *owner_off = 1;
      uint64_t lim = running ? lk_spin_ns : lk_off_spin_ns;
      if (lim > 0) {
        if ((++i & 31u) && t0) {
          cpu_relax();
          continue;
        }
        /* Every 32nd pass (and the first): clock, and -- RQ_AWARE -- whether contexts
         * in our scheduler wait for a hart; then spinning would steal this one. */
        if (!(lk_rq_aware && lithe_fork_join_current_hartless_runnable() > 0)) {
          if (t0 == 0) {
            t0 = lk_now_ns();
            budget = running ? lk_budget(mutex) : lim;
          }
          if (lk_now_ns() - t0 < budget) {
            cpu_relax();
            continue;
          }
        }
      }
    }
    if (!(s & LK_PARKED) &&
        !__atomic_compare_exchange_n(&mutex->locked, &s, s | LK_PARKED, true,
                                     __ATOMIC_RELAXED, __ATOMIC_RELAXED))
      continue;
    if (!lithe_can_block()) {
      /* Not a Lithe context (GAPS B29): kernel-yield until free. */
      while (__atomic_load_n(&mutex->locked, __ATOMIC_ACQUIRE) & LK_LOCKED)
        sched_yield();
      continue;
    }
    long token = 0;
    int r = lithe_park(&mutex->locked, lk_validate, mutex, 0, &token);
    parked = 1;
    if (r == LITHE_PARK_UNPARKED && token == LK_HANDOFF)
      break;   /* the unlocker handed us the lock (fairness) */
    t0 = 0;
    i = 0;
  }
  lk_set_owner(mutex);
  return parked;
}

static long lk_unpark_cb(void *arg, int unparked, int more, unsigned long long waited)
{
  lithe_mutex_t *m = arg;
  if (unparked && waited > lk_fair_ns) {
    __atomic_store_n(&m->locked, LK_LOCKED | (more ? LK_PARKED : 0), __ATOMIC_RELEASE);
    return LK_HANDOFF;
  }
  __atomic_store_n(&m->locked, more ? LK_PARKED : 0, __ATOMIC_RELEASE);
  return 0;
}

static inline int lot_unlock(lithe_mutex_t *mutex)
{
  __atomic_store_n(&mutex->owner, NULL, __ATOMIC_RELAXED);
  int s = LK_LOCKED;
  if (__atomic_compare_exchange_n(&mutex->locked, &s, 0, false,
                                  __ATOMIC_RELEASE, __ATOMIC_RELAXED))
    return 0;
  (void)lithe_unpark_one(&mutex->locked, lk_unpark_cb, mutex);
  return 0;
}

/* For lithe_condvar's requeue (condvar.c): mark the mutex PARKED if it is held, so its
 * unlock will unpark the requeued waiters. Returns 0 if it is not held (wake instead). */
int __lithe_mutex_lot_mark_parked(lithe_mutex_t *mutex)
{
  int s = __atomic_load_n(&mutex->locked, __ATOMIC_RELAXED);
  do {
    if (!(s & LK_LOCKED))
      return 0;
  } while (!__atomic_compare_exchange_n(&mutex->locked, &s, s | LK_PARKED, true,
                                        __ATOMIC_ACQ_REL, __ATOMIC_RELAXED));
  return 1;
}
int __lithe_mutex_is_lot(lithe_mutex_t *mutex) { return mutex_lot(mutex); }
void __lithe_mutex_lot_took_handoff(lithe_mutex_t *mutex) { lk_set_owner(mutex); }
/* ===================================================================================== */

int lithe_mutex_trylock(lithe_mutex_t *mutex)
{
  if(mutex == NULL)
    return EINVAL;
  if (mutex_lot(mutex)) {
    int z = 0;
    int ok = __atomic_compare_exchange_n(&mutex->locked, &z, LK_LOCKED, false,
                                         __ATOMIC_ACQUIRE, __ATOMIC_RELAXED);
    if (ok)
      lk_set_owner(mutex);
    if (__builtin_expect(lprof_on > 0, 0) || (lprof_on < 0 && lprof_enabled())) {
      struct lprof_ent *e = lprof_slot(__builtin_return_address(0));
      if (e) { e->acq += ok; e->try_fail += !ok; }
    }
    return ok ? 0 : EBUSY;
  }
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
  if (mutex_lot(mutex) || __builtin_expect(lprof_on != 0, 0)) {
    int prof = lprof_on > 0 || (lprof_on < 0 && lprof_enabled());
    if (mutex_lot(mutex)) {
      int z = 0;
      if (__atomic_compare_exchange_n(&mutex->locked, &z, LK_LOCKED, false,
                                      __ATOMIC_ACQUIRE, __ATOMIC_RELAXED)) {
        lk_set_owner(mutex);
        if (prof)
          lprof_record(__builtin_return_address(0), 0, 0, 0, 0);
        return 0;
      }
      int owner_off = 0;
      uint64_t t = prof ? lk_now_ns() : 0;
      int parked = lot_lock_slow(mutex, &owner_off);
      if (prof)
        lprof_record(__builtin_return_address(0), 1, parked, owner_off, lk_now_ns() - t);
      return 0;
    }
    if (prof && mutex_fast(mutex)) {
      /* Profile the B67 fast path too (contended = the first CAS failed). */
      int z = 0;
      if (__atomic_compare_exchange_n(&mutex->locked, &z, 1, false,
                                      __ATOMIC_ACQUIRE, __ATOMIC_RELAXED)) {
        mutex->owner = lithe_context_self();
        lprof_record(__builtin_return_address(0), 0, 0, 0, 0);
        return 0;
      }
      lithe_context_t *o = mutex->owner;
      int owner_off = !(o && o->uth.state == UT_RUNNING);
      uint64_t t = lk_now_ns();
      fast_lock_slow(mutex);
      lprof_record(__builtin_return_address(0), 1, 0, owner_off, lk_now_ns() - t);
      return 0;
    }
  }
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
  if (mutex_lot(mutex))
    return lot_unlock(mutex);
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

