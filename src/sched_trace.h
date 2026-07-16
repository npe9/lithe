/* Copyright (c) 2026 The Regents of the University of California
 * Lithe scheduling decision + latency tracing (env-gated, default OFF).
 *
 * Enable:  LITHE_SCHED_TRACE=1
 * Dump:    LITHE_SCHED_TRACE_PATH=/path/to/file.bin  (default: lithe_sched_trace.<pid>.bin)
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

#ifdef __cplusplus
}
#endif

#endif /* LITHE_SCHED_TRACE_H */
