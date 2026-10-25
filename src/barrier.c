/* Copyright (c) 2011 The Regents of the University of California
 * Ben Hindman <benh@cs.berkeley.edu>
 * Kevin Klues <klueska@cs.berkeley.edu>
 * See COPYING for details.
 */

#include <parlib/parlib.h>
#include <stdlib.h>
#include "internal/assert.h"
#include "barrier.h"

long lithe_fork_join_current_hartless_runnable(void);

/* LITHE_BARRIER_SPIN_DEMAND (2026-10-24, GAPS B57; default 0): the pre-block spin
 * also stops as soon as a runnable context in this scheduler has no hart, so the
 * spin never holds a hart anyone else wants. Why the spin is kept at all:
 * hosted miniFE MPI+OpenMP 32^3 (2 ranks x 2 threads, 4 harts, n=7) CG
 * 0.0153-0.0155 s with it, 0.0166-0.0168 s at LITHE_BARRIER_SPINS=0 -- in every
 * idle-path arm (FJS spin on or off, bottom window 0-200 us), so the cost is the
 * block -> unblock -> dispatch round trip itself (~2 us per rank barrier), not
 * where the idle hart waits. Remove the spin once that path is cheaper. */
static int barrier_spin_demand_on(void)
{
  static int on = -1;
  if (__builtin_expect(on < 0, 0)) {
    const char *e = getenv("LITHE_BARRIER_SPIN_DEMAND");
    on = (e && *e) ? (atoi(e) != 0) : 0;
  }
  return on;
}

void lithe_barrier_init(lithe_barrier_t *barrier, int N) 
{
  assert(barrier != NULL);
  barrier->N = N;
  barrier->arrived = 0;
  barrier->wait = false;
  barrier->signals = (padded_bool_t *)calloc(N, sizeof(padded_bool_t));
  barrier->blocked[0].queue = (lithe_context_t **)malloc(N * sizeof(lithe_context_t *));
  barrier->blocked[1].queue = (lithe_context_t **)malloc(N * sizeof(lithe_context_t *));
  barrier->blocked[0].len = 0;
  barrier->blocked[1].len = 0;
  barrier->blocked[0].maxlen = N;
  barrier->blocked[1].maxlen = N;
  mcs_pdr_init(&barrier->blocked[0].mtx);
  mcs_pdr_init(&barrier->blocked[1].mtx);
}


void lithe_barrier_destroy(lithe_barrier_t *barrier) 
{
  assert(barrier != NULL);
  free(barrier->signals);
  free(barrier->blocked[0].queue);
  free(barrier->blocked[1].queue);
}


void __lithe_barrier_block(lithe_context_t *context, void *__blocked) 
{
  contextq_t *blocked = (contextq_t *)__blocked;
  assert(blocked != NULL);
  assert(blocked->len < blocked->maxlen);
  blocked->queue[blocked->len] = context;
  blocked->len += 1;
  mcs_pdr_unlock(&blocked->mtx, blocked->qnode);
}


void lithe_barrier_wait(lithe_barrier_t *barrier) 
{
  assert(barrier != NULL);

  /* toggled signal value for barrier reuse */
  bool wait = barrier->wait;

  /* increment the counter to signal arrival */
  int id = __sync_fetch_and_add(&barrier->arrived, 1);

  /* if last to arrive at barrier, release everyone else */ 
  if (id == (barrier->N - 1)) {
    /* reset barrier */
    barrier->arrived = 0;
    rmb();
    barrier->wait = !barrier->wait;
    wmb();

    /* signal everyone that they can continue */
    int i;
    for (i = 0; i < barrier->N; i++) {
      barrier->signals[i].val = !wait;
    }

    /* unblock those that are no longer running */
    contextq_t *blocked = &barrier->blocked[wait];
    mcs_lock_qnode_t qnode = {0};
    mcs_pdr_lock(&blocked->mtx, &qnode);
    for (i = 0; i < blocked->len; i++) {
      lithe_context_unblock(blocked->queue[i]);
    }
    blocked->len = 0;
    mcs_pdr_unlock(&blocked->mtx, &qnode);
  } 
  
  /* wait for remaining to arrive */
  else {
    /* Spin only when the barrier is not oversubscribed. With more contexts
     * than vcores, spinning burns the harts that should run the missing
     * arrivals; parking lets the scheduler make progress. */
    /* LITHE_BARRIER_SPINS (2026-10-20, A/B; default 1000): spins before
     * blocking. 1000 PAUSEs are tens of us; hosted miniFE's per-iteration
     * compute is ~150 us at 32^3, so ranks blocked in every dot-product
     * barrier and paid a 230 us p50 wake->dispatch (Perfetto). */
    static int spins = -1;
    if (spins < 0) {
      const char *e = getenv("LITHE_BARRIER_SPINS");
      int v = e ? atoi(e) : 1000;
      spins = v >= 0 ? v : 1000;
    }
    const int MAXSTALLS = (barrier->N <= parlib_vcore_limit()) ? spins : 0;
    int nstalls = 0;
    const int demand = barrier_spin_demand_on();
    while (barrier->signals[id].val == wait) { 
      nstalls++;
      if (demand && nstalls < MAXSTALLS && (nstalls & 63) == 1 &&
          lithe_fork_join_current_hartless_runnable() > 0)
        nstalls = MAXSTALLS;   /* someone runnable has no hart: block now */
      if (nstalls >= MAXSTALLS) {
        contextq_t *blocked = &barrier->blocked[wait];
        mcs_lock_qnode_t qnode = {0};
        mcs_pdr_lock(&blocked->mtx, &qnode);
        if (barrier->signals[id].val == wait) {
          blocked->qnode = &qnode;
          lithe_context_block(__lithe_barrier_block, (void *)blocked);
        } else {
          mcs_pdr_unlock(&blocked->mtx, &qnode);
        }
      }
      cpu_relax();
    }
  }
}


