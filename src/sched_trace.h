/* Copyright (c) 2026 The Regents of the University of California
 * Lithe scheduling decision + latency tracing (env-gated, default OFF).
 *
 * Enable:  LITHE_SCHED_TRACE=1
 * Dump:    LITHE_SCHED_TRACE_PATH=/path/to/file.bin  (default: lithe_sched_trace.<pid>.bin; "%p" -> pid)
 * Format:  LITHE_SCHED_TRACE_FORMAT=bin|perfetto|both (default bin; perfetto = native protobuf)
 * Ring:    LITHE_SCHED_TRACE_RING=<power-of-2 records per vcore>  (default 4096)
 * Signal:  SIGUSR2 dumps rings (after init)
 *
 * See bench/hpdc_suite/LITHE_SCHED_TRACE.md in the lithe-docker superproject.
 */

#ifndef LITHE_SCHED_TRACE_H
#define LITHE_SCHED_TRACE_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Compact event codes (critical FJS decision points). */
typedef enum {
	LITHE_ST_NONE = 0,
	LITHE_ST_HART_ENTER = 1,
	LITHE_ST_DEQUEUE_LOCAL = 2,
	LITHE_ST_STEAL_ATTEMPT = 3,
	LITHE_ST_STEAL_SUCCESS = 4,
	LITHE_ST_STEAL_FAIL = 5,
	LITHE_ST_HART_GRANT = 6,
	LITHE_ST_HART_RETURN = 7,
	LITHE_ST_CTX_YIELD = 8,
	LITHE_ST_CTX_BLOCK = 9,
	LITHE_ST_CTX_UNBLOCK = 10,
	LITHE_ST_IDLE_SPIN = 11,       /* adaptive spin saw work / restart */
	LITHE_ST_IDLE_YIELD = 12,      /* hart_yield after empty spin */
	LITHE_ST_REACTOR_BLOCK = 13,   /* parlib_reactor_drive(-1) */
	LITHE_ST_PROGRESS_WAKE = 14,   /* schedule_context / hart request wake */
	LITHE_ST_CHILD_ENTER = 15,
	LITHE_ST_CHILD_EXIT = 16,
	LITHE_ST_WARM_CLAIM = 17,
	LITHE_ST_WARM_PARK = 18,
	/* Runtime-level events above the FJS (2026-10-16, Perfetto plan 2.4).
	 * All are emitted through lithe_sched_trace_emit() from the runtime that
	 * owns the event; sched is NULL. */
	LITHE_ST_OMP_FORK_BEGIN = 20,    /* master enters __kmp_fork_call; ctx=gtid */
	LITHE_ST_OMP_FORK_END = 21,      /* master leaves __kmp_join_call;  ctx=gtid */
	LITHE_ST_OMP_RUN_BEGIN = 22,     /* thread starts its microtask; ctx=gtid aux=tid */
	LITHE_ST_OMP_RUN_END = 23,
	LITHE_ST_OMP_BARRIER_BEGIN = 24, /* thread enters a barrier; ctx=gtid aux=bt (0 plain,1 forkjoin,2 reduction) */
	LITHE_ST_OMP_BARRIER_END = 25,
	LITHE_ST_REACTOR_PARK = 26,      /* uthread parks on an fd; ctx=fd aux=timeout_ms */
	LITHE_ST_REACTOR_WAKE = 27,      /* drive woke a waiter;    ctx=fd aux=events */
	LITHE_ST_REACTOR_EXPIRE = 28,    /* timed waiter expired;   ctx=fd */
	LITHE_ST_WAIT_SYNC_BLOCK = 29,   /* OMPI wait_sync: this context blocks; ctx=ctx key */
	LITHE_ST_WAIT_SYNC_WAKE = 30,    /* ... and resumes */
	LITHE_ST_USER_BEGIN = 31,        /* application slice on the ctx track; ctx=ctx key aux=user id */
	LITHE_ST_USER_END = 32,
	LITHE_ST_HART_REQUEST = 33,      /* FJS fjs_request_harts: ctx=h requested, aux=h after clamp (0 = clamped away) */
	LITHE_ST_BASE_REQUEST = 34,      /* base_hart_request: ctx=(needed<<16)|have after the add, aux=vcores woken (-1: no deficit) */
	LITHE_ST_BASE_GRANT = 35,        /* base_hart_enter decision: ctx=(needed<<16)|harts_before, aux=1 granted / 0 denied -> vcore parks */
	/* Runtime phases and resource brokers (2026-10-24). */
	LITHE_ST_PHASE_BEGIN = 36,       /* a runtime phase on the ctx track; ctx=ctx key aux=lithe_sched_trace_intern() id */
	LITHE_ST_PHASE_END = 37,
	LITHE_ST_TCM_PERMIT = 38,        /* TCM permit state: ctx=permit key aux=granted concurrency (0 = released/idle) */
	LITHE_ST_DUMP_MARKER = 255
} lithe_sched_trace_ev_t;

/* Online / offline incorrect-decision heuristic flags (OR into decision). */
#define LITHE_ST_DEC_OK                     0u
#define LITHE_ST_DEC_STEAL_LOCAL_NONEMPTY   (1u << 0) /* steal while local tqsize>0 */
#define LITHE_ST_DEC_IDLE_WITH_RUNNABLE     (1u << 1) /* idle yield runnable_count>0 */
#define LITHE_ST_DEC_GRANT_EMPTY_CHILD      (1u << 2) /* grant; child FJS runnable==0 */
#define LITHE_ST_DEC_GRANT_PARENT_RUNNABLE  (1u << 3) /* grant while parent runnable>0 */
#define LITHE_ST_DEC_YIELD_WITH_PEERS       (1u << 4) /* ctx yield; sched runnable>1 */
#define LITHE_ST_DEC_SELF_RESCHED_HOT       (1u << 5) /* high hart_enter rate (VCORE=1) */

/* Binary record (32 bytes with explicit pad). Endianness = host. */
typedef struct lithe_sched_trace_rec {
	uint64_t ns;            /* CLOCK_MONOTONIC_RAW ns (or MONOTONIC) */
	uint32_t latency_ns;    /* ns since previous event on this vcore (capped) */
	uint16_t vcore_id;
	uint8_t  ev;            /* lithe_sched_trace_ev_t */
	uint8_t  decision;      /* LITHE_ST_DEC_* bitflags */
	uint32_t sched_id;      /* truncated (uintptr_t)sched */
	uint32_t ctx_id;        /* context->id, or 0 */
	int32_t  aux;           /* runnable_count, victim vc, harts_needed, ... */
	uint32_t pad;           /* keep sizeof == 32 / natural alignment */
} lithe_sched_trace_rec_t;

/* Online counters dumped with the ring (and printable via dump). */
typedef struct lithe_sched_trace_counters {
	uint64_t events_emitted;
	uint64_t events_dropped;
	uint64_t steal_local_nonempty;
	uint64_t idle_with_runnable;
	uint64_t grant_empty_child;
	uint64_t grant_parent_runnable;
	uint64_t yield_with_peers;
	uint64_t self_resched_hot;
	uint64_t hart_enter;
	uint64_t idle_yield;
	uint64_t steal_attempt;
	uint64_t steal_success;
} lithe_sched_trace_counters_t;

#define LITHE_SCHED_TRACE_MAGIC 0x3154534cu /* 'LST1' little-endian */

typedef struct lithe_sched_trace_file_hdr {
	uint32_t magic;
	uint32_t version;       /* 1 */
	uint32_t n_vcores;
	uint32_t ring_cap;
	uint32_t rec_size;      /* sizeof(lithe_sched_trace_rec_t) */
	uint32_t reserved;
	uint64_t pid;
} lithe_sched_trace_file_hdr_t;

/* Per-vcore section header in the dump file. */
typedef struct lithe_sched_trace_vc_hdr {
	uint32_t vcore_id;
	uint32_t n_records;
	uint64_t write_seq;
	uint64_t dropped;
} lithe_sched_trace_vc_hdr_t;

/* Fast check: returns 0 when tracing is off (default). */
int lithe_sched_trace_enabled(void);

/* Emit one event. No-op when disabled. Safe from vcore context only. */
void lithe_sched_trace_emit(uint8_t ev, void *sched, uint32_t ctx_id,
                            uint8_t decision, int32_t aux);

/* Force dump of all rings + counters to LITHE_SCHED_TRACE_PATH (or default).
 * Returns 0 on success, -1 on error. */
int lithe_sched_trace_dump(void);

/* Copy online counters (always valid; zeros when never enabled). */
void lithe_sched_trace_get_counters(lithe_sched_trace_counters_t *out);

/* Event name for analyzers / debug. */
const char *lithe_sched_trace_ev_name(uint8_t ev);
/* Write the rings as a native Perfetto protobuf trace (TrackEvent packets:
 * hart threads with context/idle slices and instants, per-hart runnable
 * counters, async tracks per OpenMP thread and per MPI context, flows from
 * context_unblock to the next dispatch, a ClockSnapshot so the trace merges
 * with other MONOTONIC_RAW/BOOTTIME traces). Selected for the normal dump by
 * LITHE_SCHED_TRACE_FORMAT=perfetto (or =both: .bin plus <path>.perfetto-trace). */
int lithe_sched_trace_dump_perfetto(const char *path);
/* Stable small key for "the current context": the FJS context id when the
 * current scheduler is a fork-join scheduler, otherwise a pointer-derived key
 * with the top bit set. 0 when there is no current context. */
uint32_t lithe_sched_trace_ctx_key(void);

/* Names for the trace (2026-10-24). label_self: name the current context ("tbb worker",
 * "omp thread"); its hart slices read "ctx N <label>". intern: a small id for a phase
 * name, for LITHE_ST_PHASE_BEGIN's aux (the same name returns the same id; 0 when the
 * table is full or tracing is off). phase_begin/end: emit PHASE_BEGIN/END for the current
 * context. All are no-ops when tracing is off; call intern once and keep the id. */
void lithe_sched_trace_label_self(const char *label);
/* Tag the current context with an application rank (hosted MPI rank), for profiling
 * tools (parlib/tool.h). Kept when tracing is on or a tool is registered. */
void lithe_context_rank_self(int rank);
int32_t lithe_sched_trace_intern(const char *name);
void lithe_sched_trace_phase_begin(int32_t id);
void lithe_sched_trace_phase_end(void);

#ifdef __cplusplus
}
#endif

#endif /* LITHE_SCHED_TRACE_H */
