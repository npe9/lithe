/* Copyright (c) 2011 The Regents of the University of California
 * Ben Hindman <benh@cs.berkeley.edu>
 * Kevin Klues <klueska@cs.berkeley.edu>
 * See COPYING for details.
 */

/*
 * Lithe implementation.
 */

#ifndef __GNUC__
#error "expecting __GNUC__ to be defined (are you using gcc?)"
#endif

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <errno.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <dlfcn.h>
#include <unistd.h>

#include <parlib/parlib.h>
#include <parlib/reactor.h>
#include "lithe.h"
#include "fatal.h"
#include "internal/assert.h"
#include "internal/vcore.h"
#include "sched_trace.h"
#include <sched.h>

#ifndef __linux__
#ifndef __ros__
#error "Sorry, your operating system is not yet support by lithe (Linux and Akaros, currently)"
#endif
#endif

/* Struct to hold the function pointer and argument of a function to be called
 * in vcore_entry after one of the lithe functions yields from a context */
typedef struct lithe_vcore_func {
  void (*func) (void *);
  void *arg;
} lithe_vcore_func_t;

/* Linux Specific! (handle async syscall events) */
static void lithe_handle_syscall(struct event_msg *ev_msg, unsigned int ev_type);

/* Hooks from uthread code into lithe */
static void lithe_vcore_entry(void);
static void lithe_thread_runnable(uthread_t *uthread);
static void lithe_thread_paused(struct uthread *uthread);
static void lithe_thread_has_blocked(struct uthread *uthread, int flags);
static void lithe_blockon_sysc(struct uthread *uthread, void *sysc);

/* Unique function pointer table required by uthread code */
struct schedule_ops lithe_sched_ops = {
  .sched_entry         = lithe_vcore_entry,
  .thread_runnable     = lithe_thread_runnable,
  .thread_paused       = lithe_thread_paused,
  .thread_has_blocked  = lithe_thread_has_blocked,
  .thread_blockon_sysc = lithe_blockon_sysc,
  .preempt_pending     = NULL,
  .spawn_thread        = NULL,
};

/* A statically defined global to store the main context */
static lithe_context_t lithe_main_context;

/* A statically defined global used to assign the next id to a newly
 * initialized context */
static size_t next_context_id = 0;

/* Lithe's base scheduler functions */
static void base_hart_request(lithe_sched_t *this, lithe_sched_t *child, int h);
static void base_hart_enter(lithe_sched_t *this);
static void base_hart_return(lithe_sched_t *this, lithe_sched_t *child);
static void base_sched_entered(lithe_sched_t *this);
static void base_sched_exited(lithe_sched_t *this);
static void base_child_entered(lithe_sched_t *this, lithe_sched_t *child);
static void base_child_exited(lithe_sched_t *this, lithe_sched_t *child);
static void base_context_block(lithe_sched_t *__this, lithe_context_t *context);
static void base_context_unblock(lithe_sched_t *__this, lithe_context_t *context);
static void base_context_yield(lithe_sched_t *__this, lithe_context_t *context);
static void base_context_exit(lithe_sched_t *__this, lithe_context_t *context);

static const lithe_sched_funcs_t base_funcs = {
  .hart_request       = base_hart_request,
  .hart_enter         = base_hart_enter,
  .hart_return        = base_hart_return,
  .sched_enter        = base_sched_entered,
  .sched_exit         = base_sched_exited,
  .child_enter        = base_child_entered,
  .child_exit         = base_child_exited,
  .context_block      = base_context_block,
  .context_unblock    = base_context_unblock,
  .context_yield      = base_context_yield,
  .context_exit       = base_context_exit
};

/* Base scheduler itself */
static lithe_sched_t base_sched = { 
  .funcs  = &base_funcs,
  .harts  = PARLIB_ATOMIC_INITIALIZER(0),
  .parent = NULL,
};

/* Root scheduler, i.e. the child scheduler of base. */
static struct {
  lithe_sched_t CACHE_LINE_ALIGNED *sched;
  parlib_atomic_t CACHE_LINE_ALIGNED ref_count;
  parlib_atomic_t CACHE_LINE_ALIGNED harts_needed;
} __root_sched = {NULL, PARLIB_ATOMIC_INITIALIZER(0), PARLIB_ATOMIC_INITIALIZER(0)};

#define root_sched (__root_sched.sched)
#define root_sched_ref_count (__root_sched.ref_count)
#define root_sched_harts_needed (__root_sched.harts_needed)

static __thread struct {
  /* The next context to run on this vcore when lithe_vcore_entry is called again
   * after a yield from another lithe context */
  lithe_context_t *next_context;

  /* The current scheduler to run any contexts / functions with */
  lithe_sched_t *current_sched;

  /* Space for an arbitrary pointer of data we may wish to pass between
   * functions without using arguments */
  void *vcore_data;

} lithe_tls __attribute__((tls_model("initial-exec"))) = {NULL, NULL, NULL};
#define next_context     (lithe_tls.next_context)
#define current_sched    (lithe_tls.current_sched)
#define vcore_data       (lithe_tls.vcore_data)
#define current_context  ((lithe_context_t*)current_uthread)

static pthread_once_t lithe_lib_once = PTHREAD_ONCE_INIT;
static bool lithe_initialized = false;

static void lithe_lib_init_impl(void)
{
  /* Create a lithe context for the main thread to run in */
  lithe_context_t *context = &lithe_main_context;
  assert(context);

  /* Set the scheduler associated with the context to be the base scheduler */
  context->sched = &base_sched;

  /* Publish our sched_ops, overriding the defaults */
  sched_ops = &lithe_sched_ops;

  /* fork() support: reset schedulers in the child (see lithe_fork_child). */
  {
    extern void lithe_fork_child(int vcoreid);
    extern void parlib_register_fork_child(void (*fn)(int vcoreid));
    parlib_register_fork_child(lithe_fork_child);
  }

  /* Tell the parlib I/O reactor how to wake a uthread. The default
   * (uthread_runnable) bypasses lithe's per-context scheduler routing,
   * which trips FJS's "current_sched == ctx->sched" assertion in
   * context_unblock when the draining vcore is running a different
   * scheduler than the parked uthread. lithe_context_unblock already
   * does the correct current_sched swap (see line ~758 of this file). */
  parlib_reactor_set_unblock_fn(
      (parlib_reactor_unblock_fn_t)lithe_context_unblock);

  /* Lithe glibc: libc's own lock waits park contexts, not harts (libc_futex.c;
   * LITHE_LIBC_FUTEX=1, default off). */
  {
    extern void lithe_libc_futex_init(void);
    lithe_libc_futex_init();
  }

  /* Handle syscall events. */
  /* These functions are declared in parlib for simulating async syscalls on linux */
  ev_handlers[EV_SYSCALL] = lithe_handle_syscall;

  /* Once we have set things up for the main context, initialize the uthread
   * library with that main context */
  /* Don't initialize uthread during library loading - wait for explicit initialization */
  /* uthread_lib_init(&context->uth); */

  /* Fill in the main context stack info. */
  parlib_get_main_stack(&context->stack.bottom, &context->stack.size);

  /* Initialize vcore request/yield data structures */
  /* Don't initialize vcore during library loading - wait for explicit initialization */
  /* lithe_vcore_init(); */

  /* uthread_lib_init() above is intentionally deferred to
   * lithe_lib_init_complete() (called from MPI_Init / sched_enter), so the
   * main context has no TLS area yet. uthread_set_tls_var() expands to a
   * %fs:offset store whose target is tls_desc + tls_offset(current_sched);
   * with tls_desc==NULL that lands at a tiny absolute address and SIGSEGVs
   * the very first user that dlopens liblithe (e.g. opal_wrapper). Skip
   * the TLS seeding here; lithe_lib_init_complete() / sched_enter set
   * current_sched on real TLS once uthread_lib_init has run. */
  if (context->uth.tls_desc != NULL) {
    uthread_set_tls_var(&context->uth, current_sched, &base_sched);
  }

  lithe_initialized = true;
}

void lithe_lib_init_complete(lithe_context_t *context)
{
  /* Complete the initialization that was deferred during library loading */
  uthread_lib_init(&context->uth);
  lithe_vcore_init();
  /* uthread_lib_init() ran and assigned context->uth.tls_desc=get_main_tls().
   * lithe_lib_init_impl() couldn't seed the TLS current_sched slot because
   * tls_desc was NULL at constructor time; do it here so that when the
   * uthread next runs (or is observed by code running as that uthread, i.e.
   * the main thread), lithe_sched_current() returns &base_sched instead of
   * NULL.  Without this, lithe_sched_enter() asserts (current_sched==NULL)
   * during the OPAL sched_enter that pmix_thread_start indirectly forces. */
  if (context->uth.tls_desc != NULL) {
    uthread_set_tls_var(&context->uth, current_sched, &base_sched);
  }
}

void lithe_lib_init_real()
{
  if (lithe_initialized) {
    return;
  }
  /* debug print removed */
  init_once_racy(return);

  /* Create a lithe context for the main thread to run in */
  lithe_context_t *context = &lithe_main_context;
  assert(context);

  /* Set the scheduler associated with the context to be the base scheduler */
  context->sched = &base_sched;

  /* Publish our sched_ops, overriding the defaults */
  sched_ops = &lithe_sched_ops;

  /* Handle syscall events. */
  /* These functions are declared in parlib for simulating async syscalls on linux */
  ev_handlers[EV_SYSCALL] = lithe_handle_syscall;

  /* Once we have set things up for the main context, initialize the uthread
   * library with that main context */
  /* Don't initialize uthread during library loading - wait for explicit initialization */
  /* uthread_lib_init(&context->uth); */

  /* Fill in the main context stack info. */
  parlib_get_main_stack(&context->stack.bottom, &context->stack.size);

  /* Initialize vcore request/yield data structures */
  /* Don't initialize vcore during library loading - wait for explicit initialization */
  /* lithe_vcore_init(); */

  /* Same NULL-tls_desc guard as lithe_lib_init_impl above. */
  if (context->uth.tls_desc != NULL) {
    uthread_set_tls_var(&context->uth, current_sched, &base_sched);
  }

  lithe_initialized = true;
}

void __attribute__((constructor(98))) lithe_lib_init()
{
  pthread_once(&lithe_lib_once, lithe_lib_init_impl);
}

/* Run-once guard for lithe_ensure_main_on_vcore0().  uthread_lib_init() and
 * lithe_vcore_init() must each run exactly once for the main TCB; pthread_once
 * gives us that and serializes racing constructors. */
static pthread_once_t lithe_ensure_main_once = PTHREAD_ONCE_INIT;
static void lithe_ensure_main_on_vcore0_impl(void)
{
  /* lithe_lib_init_impl() (constructor(98)) already ran via pthread_once and
   * filled lithe_main_context except for the uthread/vcore bits. Complete
   * them now: uthread_lib_init() sets current_uthread = &context->uth and
   * gives it a TLS area; lithe_vcore_init() arms the vcore request path. */
  lithe_lib_init_complete(&lithe_main_context);
}

void lithe_ensure_main_on_vcore0(void)
{
  /* Fast path: main has already been promoted to a uthread. */
  if (current_uthread != NULL) {
    return;
  }
  /* Make sure the .so constructor body has actually run.  pthread_once on a
   * separate token from lithe_lib_once: lithe_lib_init() is normally driven
   * by the dynamic linker's _dl_init, but we may be called from a context
   * where that ordering is not guaranteed (e.g. weak OPAL hooks). */
  pthread_once(&lithe_lib_once, lithe_lib_init_impl);
  /* Do the deferred uthread/vcore bring-up exactly once for the main TCB. */
  pthread_once(&lithe_ensure_main_once, lithe_ensure_main_on_vcore0_impl);
}

static void __attribute__((noreturn)) __lithe_sched_reenter()
{
  assert(in_vcore_context());
  assert(current_sched);
  assert(current_sched->funcs);

  /* Enter current scheduler. */
  assert(current_sched->funcs->hart_enter);
  current_sched->funcs->hart_enter(current_sched);
  fatal("lithe: returned from enter");
}

static void __attribute__((noreturn)) lithe_vcore_entry()
{
  /* Make sure we are in vcore context */
  assert(in_vcore_context());

  /* If we are entering this vcore for the first time, we need to set the
   * current scheduler appropriately and up the hart count for it */
  if(current_sched == NULL) {
    /* Set the current scheduler as the base scheduler */
    current_sched = &base_sched;
    atomic_add(&base_sched.harts, 1);
  }

  /* If current_context is set, then just resume it. This will happen in one of 2
   * cases: 1) It is the first, i.e. main thread, or 2) The current vcore was
   * taken over to run a signal handler from the kernel, and is now being
   * restarted */
  if(current_context) {
    current_sched = current_context->sched;
    run_current_uthread();
    assert(0); // Should never return from running context
  }

  /* Otherwise, if next_context is set, start it off anew. */
  if(next_context) {
    lithe_context_t *context = next_context;
    current_sched = context->sched;
    next_context = NULL;
    run_uthread(&context->uth);
    assert(0); // Should never return from running context
  }

  /* Otherwise, just reenter hart_enter of whatever the current scheduler is */
  __lithe_sched_reenter();
  assert(0); // Should never return from entered scheduler
}

static void lithe_thread_runnable(uthread_t *uthread)
{
  assert(uthread);
  assert(current_sched);
  assert(current_sched->funcs->context_unblock);

  lithe_context_t *context = (lithe_context_t*)uthread;
  current_sched->funcs->context_unblock(current_sched, context);
}

static void lithe_thread_paused(struct uthread *uthread)
{
  assert(uthread);
  assert(current_sched);
  assert(current_sched->funcs);
  assert(current_sched->funcs->context_yield);
  current_sched->funcs->context_yield(current_sched, (lithe_context_t*)uthread);
}

static void lithe_thread_has_blocked(struct uthread *uthread, int flags)
{
}

static void lithe_blockon_sysc(struct uthread* uthread, void *sysc)
{
  /* Set things up so we can wake this context up later */
  ((struct syscall*)sysc)->u_data = uthread;

  /* Inform the scheduler of the block. */
  assert(current_sched);
  assert(current_sched->funcs);
  assert(current_sched->funcs->context_block);
  current_sched->funcs->context_block(current_sched, (lithe_context_t*)uthread);
}

static void lithe_handle_syscall(struct event_msg *ev_msg, unsigned int ev_type)
{
  struct syscall *sysc;
  assert(in_vcore_context());
  assert(ev_msg);

  /* Get the sysc from the message. */
  sysc = ev_msg->ev_arg3;
  assert(sysc);

  /* Extract the uthread from it. */
  struct uthread *uthread = (struct uthread*)sysc->u_data;
  assert(uthread);
  assert(uthread->sysc == sysc); /* set in uthread.c */
  uthread->sysc = 0; /* so we don't 'reblock' on this later */

  lithe_context_unblock((lithe_context_t *)uthread);
}

static void __lithe_hart_grant(lithe_sched_t *child, void (*unlock_func) (void *), void *lock);
static void base_hart_enter(lithe_sched_t *__this)
{
  // This function will never return.  Either we eventually yield the vcore
  // back to the system, or we grant it to a child scheduler.  When one of
  // these two things happens, we will break out of this infinite loop...
  while (1) {
    // B42 kernel-block compensation (PARLIB_SPARE_VCORES): once the blocked
    // vcore's thread is back, more vcores run than the cap admits; the surplus
    // leaves here, the next time any hart passes through the base scheduler,
    // instead of being granted down again.
    if (parlib_vcores_over_cap()) {
      atomic_add(&__this->harts, -1);
      current_sched = NULL;
      vcore_yield();
    }
    // We need to do a bunch of refcounting here to make sure that we are able
    // to access the root_sched before passing a hart to it.  Only if we can
    // access it and are able to up its hart count, do we even attempt to
    // transfer control to it.
    // We down the refcount only after the hart returns to the base sched in
    // base_hart_return (or just below, if we didn't actually grant the hart).
    if (parlib_atomic_add_not_zero(&root_sched_ref_count, 1)) {
      rmb(); // order operations below with the atomic_add() above

      assert(root_sched);
      size_t old_harts_granted = atomic_add(&root_sched->harts, 1);
      {
        long needed_now = parlib_atomic_read(&root_sched_harts_needed);
        int granted = (old_harts_granted + 1 <= (size_t)needed_now);
        if (lithe_sched_trace_enabled())
          lithe_sched_trace_emit(LITHE_ST_BASE_GRANT, root_sched,
                                 (uint32_t)(((needed_now & 0xffff) << 16) | (old_harts_granted & 0xffff)),
                                 0, granted);
      }
      if (old_harts_granted + 1 <= parlib_atomic_read(&root_sched_harts_needed)) {
        // Finish up our accounting
        atomic_add(&__this->harts, -1);
        // Reset the hart tls to its original state
        memset(&lithe_tls, 0, sizeof(lithe_tls));
        // And grant the hart down
        __lithe_hart_grant(root_sched, NULL, NULL);
      }
      atomic_add(&root_sched->harts, -1);
      // Release the root_sched ref (we decided not to grant a hart down)
      atomic_add(&root_sched_ref_count, -1);
    }
    // If this returns, we go back to the top of the loop and attempt to grant
    // to the root scheduler again
    atomic_add(&__this->harts, -1);
    current_sched = NULL;
    maybe_vcore_yield();
    current_sched = &base_sched;
    atomic_add(&__this->harts, 1);

    handle_events();
    cpu_relax();
  }
}

static void base_hart_return(lithe_sched_t *__this, lithe_sched_t *child)
{
  atomic_add(&root_sched_ref_count, -1);
}

static void base_sched_entered(lithe_sched_t *__this)
{
  /* Do nothing, we don't need any bookkeeping in the base scheduler */
}

static void base_sched_exited(lithe_sched_t *__this)
{
  /* Do nothing, we don't need any bookkeeping in the base scheduler */
}

static void base_child_entered(lithe_sched_t *__this, lithe_sched_t *child)
{
  assert(root_sched == NULL);
  root_sched = child;
  root_sched_ref_count = PARLIB_ATOMIC_INITIALIZER(2);
  root_sched_harts_needed = PARLIB_ATOMIC_INITIALIZER(1);
}

/* EXIT QUIESCE (2026-10-15, LITHE_FJS_EXIT_QUIESCE=1, default off for A/B):
 * the drain below waits for every hart to leave the exiting root, but
 * base_hart_enter keeps GRANTING harts to it -- add_not_zero(ref_count)
 * succeeds while any hart is still inside, and root_sched_harts_needed is
 * never cleared on exit. A hart that idle-yields out of the root is re-granted
 * straight back in, so the drain can spin indefinitely (docs/GAPS.md B14:
 * bare FJS + sched_exit, no OpenMP, hung ~15% of runs on the login node;
 * gdb: base_child_exited spinning, 2 harts parked in the root's hart_enter).
 * An exiting root needs no harts: zero its demand first. */
int lithe_sched_exit_wait_on(void)
{
  static int on = -1;
  if (on < 0) {
    const char *e = getenv("LITHE_SCHED_EXIT_WAIT");
    on = (e && *e) ? (atoi(e) != 0) : 0;
  }
  return on;
}

int lithe_sched_exit_wait_on(void);
int lithe_exit_quiesce_on(void)
{
  static int on = -1;
  if (on < 0) {
    /* Default ON since 2026-10-16: login A/B on the E5 remap proxy's clean-exit
     * path, 10 exits per arm, gate off 8/10 hung >20 s in base_child_exited,
     * gate on 0/10 (.build_logs/TESTBED_20261016.md). =0 restores the drain. */
    const char *e = getenv("LITHE_FJS_EXIT_QUIESCE");
    on = (e && e[0] == '0') ? 0 : 1;
  }
  return on;
}

static void base_child_exited(lithe_sched_t *__this, lithe_sched_t *child)
{
  assert(root_sched == child);
  if (lithe_exit_quiesce_on())
    __atomic_store_n((long *)&root_sched_harts_needed, 0, __ATOMIC_SEQ_CST);
  atomic_add(&root_sched_ref_count, -2);
  while (root_sched_ref_count)
    cpu_relax();
  root_sched = NULL;
}

/* LITHE_BASE_WAKE_PARKED_ONLY (2026-10-21, DEFAULT ON; =0 restores): the reactor poke
 * below wrote the wake eventfd on every hart request with a deficit, even with no
 * hart parked in the reactor (the only parkers are FJS harts in
 * fjs_reactor_drive_parked). perf, hosted 4 KiB halo, 4 contexts / 3 harts: 22 %
 * of the loop's samples in that syscall under lithe_context_unblock ->
 * schedule_context -> lithe_hart_request. Same fix as C16's
 * LITHE_FJS_WAKE_PARKED_ONLY for yields. A/B, 4 interleaved reps: halo 4 KiB
 * 7.6-7.8 -> 5.1-5.2 us, 8 contexts / 3 harts 11.0 -> 6.8-7.3, allreduce 5.6 -> 5.3,
 * hosted MPI+OpenMP 16^3 (2 threads, VCORE 3) CG 0.0111-0.0113 -> 0.0075-0.0079 s,
 * serial 32^3 -2 %, ranks K=8 / GPU ranks / miniFE 32^3 unchanged. Stress (with
 * nbc) ALL CLEAN at 4/3, 8/3, 8/2; 0/400 hang hunts; timed-futex reproducer 10/10. */
int lithe_fjs_reactor_parked_harts(void);
static int base_wake_parked_only = -1;
static inline int base_wake_parked_only_on(void)
{
  if (base_wake_parked_only < 0) {
    const char *e = getenv("LITHE_BASE_WAKE_PARKED_ONLY");
    base_wake_parked_only = !(e && e[0] == '0');
  }
  return base_wake_parked_only;
}

/* DIRECTED WAKE (LITHE_WAKE_HINT, GAPS B62, default 0 until A/B'd). An unblocked
 * FJS context is queued on its home vcore (preferred_vcq), like a qthreads
 * waiter going back to its home shepherd's queue. But maybe_vcore_request()
 * claimed whichever idle hart a scan from vcore 0 found first, so the hart that
 * came down found its own queue empty and had to steal from the home queue
 * (tdequeue 12 % of a cross-hart fanout profile; qthreads nemesis, whose idle
 * worker spins on the home queue itself, does the same fan-out 3.2x faster).
 * schedule_context records each woken context's home vcore here (per thread:
 * the release loop runs on the waker's stack, batched or not); the next base
 * request claims those vcores' spinning harts first. A stale hint costs a
 * steal, as before. */
#define LITHE_WAKE_HINT_MAX 16
static __thread int lithe_wake_hint_vc[LITHE_WAKE_HINT_MAX];
static __thread int lithe_wake_hint_n;
static int lithe_wake_hint_on = -1;
int lithe_wake_hint_enabled(void)
{
  if (__builtin_expect(lithe_wake_hint_on < 0, 0)) {
    const char *e = getenv("LITHE_WAKE_HINT");
    lithe_wake_hint_on = (e && e[0] == '1') ? 1 : 0;
  }
  return lithe_wake_hint_on;
}
void lithe_wake_hint(int vc)
{
  if (lithe_wake_hint_enabled() && vc >= 0 && lithe_wake_hint_n < LITHE_WAKE_HINT_MAX)
    lithe_wake_hint_vc[lithe_wake_hint_n++] = vc;
}
static int lithe_wake_hinted(int k)
{
  int n = lithe_wake_hint_n, claimed = 0;
  lithe_wake_hint_n = 0;
  for (int i = 0; i < n && claimed < k; i++) {
    int v = lithe_wake_hint_vc[i];
    if (v < (int)max_vcores() && wake_me_up[v].vcid &&
        __sync_lock_test_and_set(&wake_me_up[v].vcid, 0))
      claimed++;
  }
  return claimed;
}

static void base_hart_request(lithe_sched_t *__this, lithe_sched_t *child, int h)
{
  assert(root_sched == child);

  if (h < 0) {
    /* Floor at 0: idle-yield release must not underflow the demand ceiling. */
    for (;;) {
      long cur = parlib_atomic_read(&root_sched_harts_needed);
      long neu = cur + (long)h;
      if (neu < 0)
        neu = 0;
      if (parlib_atomic_cas(&root_sched_harts_needed, cur, neu))
        break;
    }
    return;
  }

  __sync_fetch_and_add(&root_sched_harts_needed, h);
  if (h > 0) {
    /* Only wake/claim idle vcores when we still have a grant deficit.
     * Unconditional maybe_vcore_request(h) false-claims wake_me_up flags on
     * surplus base-idle vcores even when root already holds enough harts:
     * they exit maybe_vcore_yield, fail the grant check, and grow adaptive
     * park_spin (up to ~2M) — a VCORE=96 stampede around every enqueue. */
    long have = root_sched ? parlib_atomic_read(&root_sched->harts) : 0;
    long need = parlib_atomic_read(&root_sched_harts_needed);
    long deficit = need - have;
    int wake_n = (deficit < (long)h) ? (int)deficit : h;
    if (lithe_sched_trace_enabled())
      lithe_sched_trace_emit(LITHE_ST_BASE_REQUEST, root_sched,
                             (uint32_t)(((need & 0xffff) << 16) | (have & 0xffff)),
                             0, (int32_t)(deficit < 1 ? -1 : wake_n));
    if (deficit < 1)
      return;
    /* Wake any vcore parked in parlib_reactor_drive(-1). The maybe_vcore_request
     * path below only notices spinning vcores (via the wake_me_up flag set
     * inside maybe_vcore_yield) and otherwise asks the kernel for a brand-new
     * vcore. With the I/O reactor in place, idle vcores park in epoll_wait
     * instead of spinning, so they need a separate poke (wake_efd write) to
     * notice that someone wants harts. Without this, PMIx/libevent wireup
     * deadlocks: the libevent progress uthread parks waiting for socket
     * readiness, the daemon replies, the reactor sees fd ready and calls
     * uthread_runnable -> schedule_context -> lithe_hart_request -> here.
     * The fresh vcore from vcore_request(1) below picks up the work, but
     * any already-running vcores parked in reactor_drive don't, so we burn
     * extra OS pthreads on every wireup. parlib_reactor_wake() is a single
     * eventfd write — cheap. */
    if (base_wake_parked_only_on()) {
      /* Pairs with fjs_reactor_drive_parked: the parker increments
       * parked_harts and then rechecks the run queue; we enqueued before
       * getting here, fence, then read the count (Dekker, as in C16). */
      __atomic_thread_fence(__ATOMIC_SEQ_CST);
      if (lithe_fjs_reactor_parked_harts() > 0)
        parlib_reactor_wake();
    } else {
      parlib_reactor_wake();
    }
    if (lithe_wake_hint_n > 0)
      wake_n -= lithe_wake_hinted(wake_n);
    if (wake_n > 0)
      maybe_vcore_request(wake_n);
  }
}

/* 1 when the CURRENT context may cooperatively BLOCK.
 *
 * False on the base scheduler -- whose context_block is a fatal() by design,
 * because a base context has nowhere to be parked -- and false when there is no
 * current scheduler at all. Those are exactly the states a process is in during
 * library CONSTRUCTORS and during _dl_fini DESTRUCTORS, after the application
 * has already exited its own scheduler.
 *
 * Added 2026-10-14 for a measured, deterministic failure: libomp's library
 * destructor reaps its workers with a lithe condvar
 * (__kmp_reap_worker -> lithe_condvar_wait), running on main's context, which
 * _dl_fini time has returned to the base scheduler. Result:
 * "Contexts created by the base scheduler should never need to block!" and
 * exit status 1, 20/20 runs. Composed runtimes must test this before using any
 * lithe blocking primitive from a path that can run at process teardown.
 *
 * Companion to parlib_can_cooperatively_yield(), which answers the same
 * question for YIELDING. Blocking is strictly stronger: it also needs a
 * scheduler willing to take the context. */
int lithe_can_block(void)
{
  lithe_sched_t *s = lithe_sched_current();

  if (s == NULL || s == &base_sched)
    return 0;
  return parlib_can_cooperatively_yield();
}

long lithe_root_harts_needed(void)
{
  return parlib_atomic_read(&root_sched_harts_needed);
}

static void base_context_block(lithe_sched_t *__this, lithe_context_t *context)
{
  fatal("Contexts created by the base scheduler should never need to block!\n");
}

static void base_context_unblock(lithe_sched_t *__this, lithe_context_t *context)
{
  fatal("Contexts created by the base scheduler should never need to unblock!\n");
}

static void base_context_yield(lithe_sched_t *__this, lithe_context_t *context)
{
  next_context = context;
}

static void base_context_exit(lithe_sched_t *__this, lithe_context_t *context)
{
  fatal("The base context should never be exiting!\n");
}

lithe_sched_t *lithe_sched_current()
{
  return current_sched;
}

static void __lithe_hart_grant(lithe_sched_t *child, void (*unlock_func) (void *), void *lock)
{
  current_sched = child;
  if(unlock_func != NULL)
    unlock_func(lock);
  vcore_reenter(__lithe_sched_reenter);
  fatal("lithe: returned from enter");
}

/* EXIT BLOCK (LITHE_SCHED_EXIT_BLOCK, 2026-10-24, GAPS B57; default 0).
 * lithe_sched_exit's drain was a yield loop (up to 2M yields) on the parent
 * context: it stayed runnable and took a hart in turns with real work until the
 * exiting child's other harts left. =1: the parent context BLOCKS; the hart
 * that takes the child's count to 0 unblocks it (exit_waiters_wake, called
 * after every decrement of a sched's harts that can happen while it exits). */
#define EXIT_WAITERS_MAX 64
static struct { lithe_sched_t *sched; lithe_context_t *ctx; } exit_waiters[EXIT_WAITERS_MAX];
static int exit_waiters_n;
static spin_pdr_lock_t exit_waiters_lock = SPINPDR_INITIALIZER;

static int lithe_sched_exit_block_on(void)
{
  static int on = -1;
  if (__builtin_expect(on < 0, 0)) {
    const char *e = getenv("LITHE_SCHED_EXIT_BLOCK");
    on = (e && *e) ? (atoi(e) != 0) : 0;
  }
  return on;
}

static inline void exit_waiters_wake(lithe_sched_t *sched)
{
  if (__builtin_expect(__atomic_load_n(&exit_waiters_n, __ATOMIC_ACQUIRE) == 0, 1))
    return;
  if (parlib_atomic_read(&sched->harts) != 0)
    return;
  lithe_context_t *ctx = NULL;
  spin_pdr_lock(&exit_waiters_lock);
  for (int i = 0; i < EXIT_WAITERS_MAX; i++)
    if (exit_waiters[i].sched == sched) {
      ctx = exit_waiters[i].ctx;
      exit_waiters[i].sched = NULL;
      exit_waiters[i].ctx = NULL;
      __atomic_fetch_sub(&exit_waiters_n, 1, __ATOMIC_RELEASE);
      break;
    }
  spin_pdr_unlock(&exit_waiters_lock);
  if (ctx)
    lithe_context_unblock(ctx);
}

/* Block callback (vcore context): register, then recheck, so a decrement that
 * raced ahead of the registration is not lost. Returns 0 if the registry was
 * full (caller falls back to the yield drain). */
static void exit_block_cb(lithe_context_t *ctx, void *arg)
{
  lithe_sched_t *sched = arg;
  int slot = -1;
  spin_pdr_lock(&exit_waiters_lock);
  for (int i = 0; i < EXIT_WAITERS_MAX; i++)
    if (exit_waiters[i].sched == NULL) {
      exit_waiters[i].sched = sched;
      exit_waiters[i].ctx = ctx;
      __atomic_fetch_add(&exit_waiters_n, 1, __ATOMIC_SEQ_CST);
      slot = i;
      break;
    }
  spin_pdr_unlock(&exit_waiters_lock);
  if (slot < 0) {
    lithe_context_unblock(ctx); /* full: run again, the caller yields instead */
    return;
  }
  exit_waiters_wake(sched);   /* count may already be 0 */
}

void lithe_hart_grant(lithe_sched_t *child, void (*unlock_func) (void *), void *lock)
{
  assert(child);
  assert(in_vcore_context());
  assert(child != &base_sched);

  lithe_sched_t *parent = current_sched;

  /* Leave parent, join child. */
  atomic_add(&parent->harts, -1);
  atomic_add(&child->harts, 1);
  exit_waiters_wake(parent);

  /* Enter the child scheduler */
  __lithe_hart_grant(child, unlock_func, lock);
}

void lithe_hart_yield()
{
  /* debug print removed */
  assert(in_vcore_context());
  assert(current_sched);
  assert(current_sched != &base_sched);

  lithe_sched_t *parent = current_sched->parent;
  lithe_sched_t *child = current_sched;

  /* Switch to parent scheduler and notify it of the hart return */
  current_sched = parent;
  assert(current_sched);
  assert(current_sched->funcs);
  assert(current_sched->funcs->hart_return);
  current_sched->funcs->hart_return(current_sched, child);

  /* Leave child, reenter on parent. */
  atomic_add(&child->harts, -1);
  atomic_add(&parent->harts, 1);
  exit_waiters_wake(child);

  vcore_reenter(__lithe_sched_reenter);
  fatal("lithe: returned from hart yield");
}

static void __lithe_sched_enter(uthread_t *uthread, void *__arg)
{
  assert(in_vcore_context());

  /* Unpack the arguments to this function */
  lithe_context_t *context = (lithe_context_t*)uthread;
  struct { 
    lithe_sched_t *parent;
    lithe_sched_t *child;
  } *arg = __arg;
  lithe_sched_t *parent = arg->parent;
  lithe_sched_t *child = arg->child;

  assert(context);
  assert(parent);
  assert(child);

  /* Officially grant the hart to the child */
  atomic_add(&parent->harts, -1);
  atomic_add(&child->harts, 1);

  /* Set the scheduler to the child in the context that was blocked */
  context->sched = child;
  uthread_set_tls_var(&context->uth, current_sched, child);

  /* Inform parent. */
  assert(parent->funcs->child_enter);
  parent->funcs->child_enter(parent, child);

  /* Update the value of current_sched on this core. */
  current_sched = child;

  /* Inform child. */
  assert(child->funcs->sched_enter);
  child->funcs->sched_enter(child);

  /* Returning from this function will bring us back to the vcore_entry point.
   * We set next_context to continue running the child context now that it has
   * been properly set up. */
  next_context = context;
}

void lithe_sched_enter(lithe_sched_t *child)
{
  /* debug print removed */
  assert(!in_vcore_context());
  assert(current_sched);
  lithe_sched_t *parent = current_sched;

  /* Make sure that the childs 'funcs' and 'main_context' have already been set
   * up properly */
  assert(child->funcs);
  assert(child->main_context);

  /* Set-up child scheduler */
  child->harts = PARLIB_ATOMIC_INITIALIZER(0);
  child->parent = parent;

  /* Set up a function to run in vcore context to inform the parent that the
   * child has taken over */
  struct { 
    lithe_sched_t *parent;
    lithe_sched_t *child;
  } arg = {parent, child};

  /* Hijack the current context and replace it with the child->main_context.
   * Store the original context in child->parent_context so we can restore it
   * on exit. */
  child->parent_context = current_context;
  hijack_current_uthread(&child->main_context->uth);

  /* Yield this context to vcore context to run the function we just set up. Once
   * we return from the yield we will be fully inside the child scheduler. */
  uthread_yield(true, __lithe_sched_enter, &arg);
}

void __lithe_sched_exit(uthread_t *uthread, void *arg)
{
  assert(in_vcore_context());

  /* Unpack the arguments to this function */
  lithe_context_t *context = (lithe_context_t*)uthread;
  struct { 
    lithe_sched_t *parent;
    lithe_sched_t *child;
  } *real_arg = arg;
  lithe_sched_t *parent = real_arg->parent;
  lithe_sched_t *child = real_arg->child;

  assert(context);
  assert(parent);
  assert(child);

  /* Update child's hart count to 0 */
  atomic_add(&child->harts, -1);
  atomic_add(&parent->harts, 1);

  /* Set the scheduler to the parent in the context that was blocked */
  context->sched = parent;
  uthread_set_tls_var(&context->uth, current_sched, parent);

  /* Inform the child. */
  assert(child->funcs->sched_exit);
  child->funcs->sched_exit(child);

  /* Update the value of current_sched on this core. */
  current_sched = parent;

  /* Inform the parent that the child scheduler has exited. */
  assert(parent->funcs->child_exit);
  parent->funcs->child_exit(parent, child);

  /* Return to the original parent context */
  next_context = context;
}

void lithe_sched_exit()
{
  /* debug print removed */
  assert(!in_vcore_context());
  assert(current_sched);

  lithe_sched_t *parent = current_sched->parent;
  lithe_sched_t *child = current_sched;

  /* Set ourselves up to jump to vcore context and run __lithe_sched_exit() */
  struct { 
    lithe_sched_t *parent;
    lithe_sched_t *child;
  } arg = {parent, child};

  /* Hijack the current context and replace it with the original parent context */
  hijack_current_uthread(&child->parent_context->uth);

  /* Yield this context to vcore context to run the function we just set up. Once
   * we return from the yield we will be fully back in the parent scheduler
   * running the original parent context. */
  uthread_yield(true, __lithe_sched_exit, &arg);

  /* Don't actually allow this context to continue until all of the child's
   * harts have been yielded. This field is synchronized with an update to its
   * value in lithe_hart_grant() as protected by a parent-scheduler-specific
   * locking function. 
   * Also, do a full blown lithe context yield so that this hart can do useful
   * work while waiting.
   * Bound the drain: hosted RPH>1 teardown can leave a stray child hart
   * count that never reaches 0; continuing is preferable to wedging Finalize. */
  long n;
  int drain_spins = 0;
  int wait = lithe_sched_exit_wait_on();
  struct timespec t_start;
  clock_gettime(CLOCK_MONOTONIC, &t_start);
  while ((n = parlib_atomic_read(&child->harts)) != 0) {
    assert(n >= 0);
    if (lithe_sched_exit_block_on() && lithe_can_block()) {
      lithe_context_block(exit_block_cb, child);
      continue;   /* woken at 0 (or the registry was full): recheck */
    }
    if (wait) {
      /* EXIT WAIT (2026-10-17, LITHE_SCHED_EXIT_WAIT): the old drain
       * yield-spun ~4 us per iteration until the child's other harts left,
       * and they leave only when their timed idle park (1->64 ms) expires or
       * a reactor poke reaches them; sched_exit sends one poke, which wakes
       * one sleeper. Perfetto audit: 50-470 ms of teardown per run with tens
       * of thousands of 0.35 us dispatches and steal attempts
       * (docs/AUDIT-perfetto-performance-20261017.md section 5). Poke on
       * every pass (each poke wakes one parked hart, which sees
       * teardown_quiesce and leaves) and sleep instead of spinning. */
      struct timespec nap = { 0, 20000 }, now;
      parlib_reactor_wake();
      nanosleep(&nap, NULL);
      clock_gettime(CLOCK_MONOTONIC, &now);
      if ((now.tv_sec - t_start.tv_sec) * 1000000000L + (now.tv_nsec - t_start.tv_nsec) > 10000000000L)
        break; /* stray hart count (hosted RPH>1, B14): continue rather than wedge */
    } else {
      if (drain_spins++ >= 2000000)
        break;
      lithe_context_yield();
    }
  }
}

struct lithe_fork_join_sched;
struct lithe_fork_join_sched *lithe_root_fork_join_sched_if_any(void);

void lithe_hart_request(int h)
{
  if (current_sched == NULL) {
    /* A foreign thread (a plain pthread, e.g. a Python thread calling into an OpenMP
     * runtime) has no current scheduler. The runtimes put such a caller's workers in the
     * process's root fork-join scheduler, so attribute the demand there. Lithe libomp's
     * fork barrier reached here and dereferenced NULL (PyTorch concurrent inference,
     * scikit-learn under joblib threads; bench/ai/omp_foreign_roots.c). */
    lithe_sched_t *root = (lithe_sched_t *)lithe_root_fork_join_sched_if_any();
    if (root != NULL)
      lithe_hart_request_for(root, h);
    return;
  }
  lithe_sched_t *parent = current_sched->parent;
  lithe_sched_t *child = current_sched;

  current_sched = parent;
  assert(parent->funcs->hart_request);
  parent->funcs->hart_request(parent, child, h);
  current_sched = child;
}

void lithe_hart_request_for(lithe_sched_t *sched, int h)
{
  lithe_sched_t *saved;

  assert(sched);
  if (sched == current_sched) {
    lithe_hart_request(h);
    return;
  }
  /* Temporarily attribute demand to @sched so parent->hart_request and any
   * recursive lithe_hart_request() see the owning child, not the caller. */
  saved = current_sched;
  current_sched = sched;
  lithe_hart_request(h);
  current_sched = saved;
}

static void __lithe_context_yield(uthread_t *uthread, void *arg)
{
  assert(uthread);
  assert(current_sched);
  assert(current_sched->funcs);
  assert(in_vcore_context());

  lithe_context_t *context = (lithe_context_t*)uthread;
  assert(current_sched->funcs->context_yield);
  current_sched->funcs->context_yield(current_sched, context);
}

extern void parlib_libc_context_gone(void *tcb);   /* parlib libc_lithe.c */

static void __lithe_context_finished(uthread_t *uthread, void *__arg)
{
  assert(uthread);
  assert(current_sched);
  assert(current_sched->funcs);
  assert(in_vcore_context());

  lithe_context_t *context = (lithe_context_t*)uthread;
  /* No longer a live context for signal-mask accounting (parlib libc_lithe.c): a finished
   * context may sit in a cache with its TLS until reused. */
  parlib_libc_context_gone(uthread->tls_desc);
  assert(current_sched->funcs->context_exit);
  current_sched->funcs->context_exit(current_sched, context);
}

static void __lithe_context_start()
{
  assert(current_context);
  assert(current_context->start_func);
  current_context->start_func(current_context->start_func_arg);

  uthread_yield(false, __lithe_context_finished, NULL);
}

static inline void __lithe_context_fields_init(lithe_context_t *context, lithe_sched_t *sched)
{
  context->start_func = NULL;
  context->start_func_arg = NULL;
  context->sched = sched;
  uthread_set_tls_var(&context->uth, current_sched, sched);
}

static inline void __lithe_context_reinit(lithe_context_t *context, lithe_sched_t *sched)
{
  /* Renitialize the new context as a uthread */
  uthread_init(&context->uth);

  /* Assign the context a new id */
  context->id = next_context_id++;

  /* Initialize the fields associated with a lithe context */
  __lithe_context_fields_init(context, sched);
}

static inline void __lithe_context_set_entry(lithe_context_t *context, 
                                             void (*func) (void *), void *arg)
{
  assert(context->stack.bottom);
  assert(context->stack.size);

  context->start_func = func;
  context->start_func_arg = arg;
  init_uthread_tf(&context->uth, __lithe_context_start, 
                  context->stack.bottom, context->stack.size);
}

void lithe_context_init(lithe_context_t *context, void (*func) (void *), void *arg)
{
  /* Assert that this is a valid context */
  assert(context);

  /* Zero out the uthread struct before passing it down: required by
   * uthread_init */
  memset(&context->uth, 0, sizeof(uthread_t));
  __lithe_context_reinit(context, current_sched);

  /* Initialize context start function and stack */
  __lithe_context_set_entry(context, func, arg);
}

void lithe_context_reinit(lithe_context_t *context, void (*func) (void *), void *arg)
{
  /* Assert that this is a valid context */
  assert(context);

  /* Call the internal lithe context reinit */
  __lithe_context_reinit(context, current_sched);

  /* Initialize context start function and stack */
  __lithe_context_set_entry(context, func, arg);
}

void lithe_context_recycle(lithe_context_t *context, void (*func) (void *), void *arg)
{
  /* Assert that this is a valid context */
  assert(context);

  /* Initialize the fields associated with a lithe context */
  __lithe_context_fields_init(context, current_sched);

  /* Initialize context start function and stack */
  __lithe_context_set_entry(context, func, arg);
}

void lithe_context_reassociate(lithe_context_t *context, lithe_sched_t *sched)
{
  context->sched = sched;
  uthread_set_tls_var(&context->uth, current_sched, sched);
}

void lithe_context_cleanup(lithe_context_t *context)
{
  assert(context);
  if(!in_vcore_context())
    assert(context != current_context);
  uthread_cleanup(&context->uth);
}

lithe_context_t *lithe_context_self()
{
  return current_context;
}

int lithe_context_run(lithe_context_t *context)
{
  assert(context);
  assert(in_vcore_context());
  /* debug print removed */

  next_context = context;
  lithe_vcore_entry();
  return -1;
}

void __lithe_context_block(uthread_t *uthread, void *__arg)
{
  assert(__arg);
  assert(in_vcore_context());

  struct { 
    void (*func) (lithe_context_t *, void *); 
    void *arg;
    parlib_atomic_t safe_to_unblock;
  } *arg = __arg;

  /* Inform the scheduler of the block first. */
  assert(current_sched);
  assert(current_sched->funcs);
  assert(current_sched->funcs->context_block);
  current_sched->funcs->context_block(current_sched, (lithe_context_t*)uthread);

  /* Then carry out the call-site specific callback to do the blocking. */
  if (arg->func)
    arg->func((lithe_context_t*)uthread, arg->arg);

  parlib_atomic_set(&arg->safe_to_unblock, 1);
}
 
void lithe_context_block(void (*func)(lithe_context_t *, void *), void *arg)
{
  assert(!in_vcore_context());
  assert(current_context);

  struct { 
    void (*func) (lithe_context_t *, void *); 
    void *arg;
    parlib_atomic_t safe_to_unblock;
  } __arg = {func, arg, 0};
  uthread_yield(true, __lithe_context_block, &__arg);
  while(parlib_atomic_read(&__arg.safe_to_unblock) == 0);
}

/* fork() child (parlib vcore.c drives it; 2026-10-22). Only the forking
 * context exists in a child: every other context's TLS owner (its backing
 * pthread) is gone and glibc has recycled that memory, and in CPython's model
 * those threads have vanished. They must never run again -- including when a
 * waiter list, futex word or timer later "wakes" one (releasing a lock that a
 * vanished thread was waiting on). Context ids come from a counter, so the
 * parent's contexts are exactly id < lithe_fork_thresh, minus the forker. */
static int lithe_fork_thresh = 0;
static int lithe_fork_keep = -1;

static inline int lithe_context_is_stale(lithe_context_t *c)
{
  return lithe_fork_thresh > 0 && c->id < lithe_fork_thresh &&
         c->id != lithe_fork_keep;
}

extern void lithe_fork_join_after_fork(lithe_sched_t *root, lithe_sched_t *cur,
                                       int vcoreid, lithe_context_t *keep);

void lithe_fork_child(int vcoreid)
{
  lithe_context_t *cur = vcoreid >= 0 ? current_context : NULL;
  lithe_fork_thresh = (int)next_context_id;
  lithe_fork_keep = cur ? cur->id : -1;
  {
    extern void lithe_futex_after_fork(void);
    extern void lithe_libc_futex_after_fork(void);
    lithe_futex_after_fork();
    lithe_libc_futex_after_fork();
  }
  /* Hart accounting: the forking hart (if any) is the only one, owned by the
   * forker's scheduler; base and root demand start from that. */
  base_sched.harts = (parlib_atomic_t)PARLIB_ATOMIC_INITIALIZER(0);
  int root_on_chain = 0;
  for (lithe_sched_t *s = cur ? cur->sched : NULL; s; s = s->parent)
    if (s == root_sched)
      root_on_chain = 1;
  root_sched_harts_needed = (parlib_atomic_t)PARLIB_ATOMIC_INITIALIZER(root_on_chain);
  if (root_sched)
    lithe_fork_join_after_fork(root_sched, cur ? cur->sched : NULL, vcoreid, cur);
}

void lithe_context_unblock(lithe_context_t *context)
{
  assert(context);
  if (context == NULL || lithe_context_is_stale(context))
    return;   /* a parent-process context in a fork() child: never runs */
  lithe_sched_t *sched = current_sched;
  current_sched = context->sched;
  uthread_runnable(&context->uth);
  current_sched = sched;
}

/* LITHE_YIELD_CALLERS=1 (2026-10-17, audit tool): histogram of the return
 * addresses of lithe_context_yield, symbolised with dladdr and printed to
 * stderr at exit. Answers "who is yielding in this storm" without a debugger
 * (the Perfetto trace records the yield, not its caller). */
static int yc_on = -1;
#define YC_SLOTS 64
static struct { void *addr; unsigned long n; } yc_tab[YC_SLOTS];
static void yc_dump(void)
{
  int i, j;
  /* sort by count, descending (tiny table) */
  for (i = 0; i < YC_SLOTS; i++)
    for (j = i + 1; j < YC_SLOTS; j++)
      if (yc_tab[j].n > yc_tab[i].n) { __typeof__(yc_tab[0]) t = yc_tab[i]; yc_tab[i] = yc_tab[j]; yc_tab[j] = t; }
  fprintf(stderr, "[LITHE_YIELD_CALLERS] pid %d: lithe_context_yield callers\n", (int)getpid());
  for (i = 0; i < YC_SLOTS && yc_tab[i].n; i++) {
    Dl_info di; const char *sym = "?", *obj = "?"; unsigned long off = 0;
    if (dladdr(yc_tab[i].addr, &di)) {
      if (di.dli_sname) { sym = di.dli_sname; off = (unsigned long)yc_tab[i].addr - (unsigned long)di.dli_saddr; }
      if (di.dli_fname) { obj = strrchr(di.dli_fname, '/'); obj = obj ? obj + 1 : di.dli_fname; }
    }
    fprintf(stderr, "  %10lu  %s+0x%lx (%s+0x%lx) %p\n", yc_tab[i].n, sym, off, obj,
            (unsigned long)yc_tab[i].addr - (unsigned long)(dladdr(yc_tab[i].addr, &di) ? di.dli_fbase : 0), yc_tab[i].addr);
  }
}
static inline void yc_record(void *ra)
{
  int i;
  if (yc_on < 0) {
    const char *e = getenv("LITHE_YIELD_CALLERS");
    yc_on = (e && *e && atoi(e) != 0) ? 1 : 0;
    if (yc_on) atexit(yc_dump);
  }
  if (!yc_on) return;
  for (i = 0; i < YC_SLOTS; i++) {
    void *a = __atomic_load_n(&yc_tab[i].addr, __ATOMIC_ACQUIRE);
    if (a == ra) { __atomic_fetch_add(&yc_tab[i].n, 1, __ATOMIC_RELAXED); return; }
    if (a == NULL) {
      void *expect = NULL;
      if (__atomic_compare_exchange_n(&yc_tab[i].addr, &expect, ra, false, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE) || expect == ra) {
        __atomic_fetch_add(&yc_tab[i].n, 1, __ATOMIC_RELAXED); return;
      }
    }
  }
}

void lithe_context_yield()
{
  /* Called from a thread that is not running a Lithe context (a parlib offload
   * helper running a lithified library's wait loop, GAPS B29: cxip_pte_set_state_wait
   * during a parallel endpoint close), there is no context to yield: yield the
   * kernel thread instead of dereferencing a NULL current context. */
  if (current_context == NULL || in_vcore_context()) {
    sched_yield();
    return;
  }
  assert(!in_vcore_context());
  assert(current_sched);
  assert(current_context);
  yc_record(__builtin_return_address(0));

  uthread_yield(true, __lithe_context_yield, NULL);
}

void lithe_context_exit()
{
  assert(!in_vcore_context());
  assert(current_sched);
  assert(current_context);

  uthread_yield(false, __lithe_context_finished, NULL);
}

extern void uthread_default_vcore_entry(void);
__attribute__((weak)) void vcore_entry(void)
{
  uthread_default_vcore_entry();
}

