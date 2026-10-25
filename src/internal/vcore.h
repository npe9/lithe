#ifndef LITHE_INTERNAL_VCORE_H
#define LITHE_INTERNAL_VCORE_H

#include <stdlib.h>
#include <time.h>
#include <sys/queue.h>
#include <parlib/vcore.h>
#include "assert.h"

/* Per-vcore idle-park state. `vcid` is the user-level wake flag claimed by
 * maybe_vcore_request() (a memory write -- no futex). `spin_budget` is the
 * adaptive park-hysteresis window: how long an idle vcore spins (cpu_relax,
 * checking the flag) before falling through to parlib vcore_yield() (the
 * sanctioned futex park). Padded to a cache line so adjacent vcores don't
 * false-share their flag/budget. */
static struct {
  int vcid;
  unsigned int spin_budget;
} __attribute__((aligned(ARCH_CL_SIZE))) *wake_me_up;

/* Park hysteresis policy (lithe/parlib substrate -- see
 * .cursor/rules/lithified-runtimes-no-linux-primitives.mdc: parlib owns the
 * futex park; here we only spin at user level longer before reaching it).
 *
 * Why: at an OpenMP barrier each idle worker context blocks (lithe condvar ->
 * lithe_hart_request(-1)); its backing vcore then runs out of work and, with a
 * tiny spin window, immediately parks in parlib futex_wait. The next fork must
 * futex_wakeup it. miniFE CG issues thousands of barriers -> a futex park/wake
 * storm (~37k futex calls / ~90% of syscall time at OMP=8, 128^3). Keeping the
 * vcore spinning for a window LONGER than the futex wake latency (~hundreds of
 * us) lets maybe_vcore_request() re-claim it with a single memory write (the
 * vcid flag) instead of a kernel park/wake pair -- a user-level wake.
 *
 * Adaptive + bounded (composition guard): the per-vcore budget GROWS when work
 * reappears during the spin (the flag was claimed -> worth staying hot) and
 * SHRINKS when the vcore spins out and genuinely had to park (idle -> park
 * sooner, releasing the hart to sibling schedulers / MPI-PMIx-UCX progress at
 * coarse granularity). Bounded by LITHE_VCORE_PARK_SPIN_{MIN,MAX}; the upper
 * bound ensures idle workers never hold a hart forever (unlike
 * KMP_BLOCKTIME=infinite, which starves MPI progress).
 *
 * Back-compat: LITHE_SPIN_COUNT (if set) pins a FIXED spin count and disables
 * adaptation (used by sweeps). LITHE_VCORE_PARK_ADAPTIVE=0 also forces fixed at
 * the MIN. */
static unsigned int park_spin_min = 8192u;
static unsigned int park_spin_max = (1u << 21); /* ~2M cpu_relax: > futex wake latency */
static int park_spin_fixed = -1;                /* >=0: fixed window (LITHE_SPIN_COUNT) */
/* LITHE_VCORE_PARK_SPIN_US (2026-10-24, GAPS B57; unset = the pause-count window
 * above). This is the only spin the de-spin rules allow (the bottom of the
 * hierarchy, before the kernel park), but 2^21 pauses is ~38 ms of a burned core
 * per idle episode, doubling on every quick reclaim -- far beyond a futex wake
 * (tens of us). Set, it caps the window in wall time (0 = park at once). */
static long park_spin_ns = 20000L; /* default 20 us since 2026-10-24 */
static int park_adaptive = 1;

static inline unsigned int __env_uint(const char *name, unsigned int dflt)
{
  const char *s = getenv(name);
  if (!s || !*s)
    return dflt;
  long v = atol(s);
  if (v < 0)
    return dflt;
  return (unsigned int)v;
}

static inline void lithe_vcore_init()
{
  wake_me_up = parlib_aligned_alloc(PGSIZE,
            sizeof(wake_me_up[0]) * max_vcores());
  assert(wake_me_up);
  memset(wake_me_up, 0, sizeof(wake_me_up[0]) * max_vcores());

  park_spin_min = __env_uint("LITHE_VCORE_PARK_SPIN_MIN", park_spin_min);
  park_spin_max = __env_uint("LITHE_VCORE_PARK_SPIN_MAX", park_spin_max);
  if (park_spin_max < park_spin_min)
    park_spin_max = park_spin_min;

  const char *adaptive_string = getenv("LITHE_VCORE_PARK_ADAPTIVE");
  if (adaptive_string != NULL)
    park_adaptive = atoi(adaptive_string) ? 1 : 0;

  {
    const char *us = getenv("LITHE_VCORE_PARK_SPIN_US");
    /* <0 restores the pure pause-count window. DEFAULT 20 us: with the FJS no
     * longer spinning (LITHE_FJS_IDLE_DIRECT=2) this is the only idle spin; a
     * blocked lithe_barrier waiter costs 0.75 us at >= 20 us vs 1.33 us at 0
     * (kernel park + futex wake), 2 contexts / 4 harts (barlat.c). */
    if (us && *us)
      park_spin_ns = atol(us) < 0 ? -1 : atol(us) * 1000L;
  }

  /* LITHE_SPIN_COUNT: legacy fixed window (overrides adaptive). */
  const char *spin_count_string = getenv("LITHE_SPIN_COUNT");
  if (spin_count_string != NULL) {
    long v = atol(spin_count_string);
    park_spin_fixed = (v < 0) ? 0 : (int)v;
  }

  /* Seed each vcore's adaptive budget at MIN; it grows toward MAX within a few
   * barriers of a hot compute region (doubles on each quick re-claim). */
  for (size_t i = 0; i < (size_t)max_vcores(); i++)
    wake_me_up[i].spin_budget = park_spin_min;
}

static inline void maybe_vcore_yield()
{
  int vc = vcore_id();
  int *flag = &wake_me_up[vc].vcid;

  *flag = 1;

  unsigned int budget;
  if (park_spin_fixed >= 0)
    budget = (unsigned int)park_spin_fixed;       /* legacy LITHE_SPIN_COUNT */
  else if (!park_adaptive)
    budget = park_spin_min;
  else
    budget = wake_me_up[vc].spin_budget;          /* adaptive per-vcore window */

  unsigned int spins;
  if (park_spin_ns >= 0) {
    struct timespec t0, t;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    for (spins = 0; spins < budget && *flag; spins++) {
      if ((spins & 255u) == 0) {
        clock_gettime(CLOCK_MONOTONIC, &t);
        if ((t.tv_sec - t0.tv_sec) * 1000000000L + (t.tv_nsec - t0.tv_nsec) >= park_spin_ns)
          break;
      }
      cpu_relax();
    }
  } else {
    for (spins = 0; spins < budget && *flag; spins++)
      cpu_relax();
  }

  if (*flag && __sync_lock_test_and_set(flag, 0)) {
    /* Spun out without being claimed: this vcore is genuinely idle. Shrink the
     * window (park sooner next time, freeing the hart) and park in parlib. */
    if (park_spin_fixed < 0 && park_adaptive) {
      unsigned int nb = budget >> 1;
      if (nb < park_spin_min)
        nb = park_spin_min;
      wake_me_up[vc].spin_budget = nb;
    }
    vcore_yield();
  } else {
    /* Claimed during the spin (maybe_vcore_request cleared the flag with a
     * memory write -- no futex): work reappeared quickly, so grow the window to
     * stay hot for the next barrier. */
    if (park_spin_fixed < 0 && park_adaptive) {
      unsigned int nb = budget ? (budget << 1) : park_spin_min;
      if (nb > park_spin_max)
        nb = park_spin_max;
      wake_me_up[vc].spin_budget = nb;
    }
  }
}

static inline void maybe_vcore_request(int k)
{
  /* Try and wake up one of our spinning vcores. */
  for (int i = 0; i < max_vcores() && k > 0; i++)
    if (wake_me_up[i].vcid && __sync_lock_test_and_set(&wake_me_up[i].vcid, 0))
      k--;

  for (int i = 0; i < k; i++)
    if (vcore_request(1) < 0)
      break;
}

#endif
