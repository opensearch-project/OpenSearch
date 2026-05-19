/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * VectorSchemaRoot (VSR) lifecycle management for Arrow-based batching.
 *
 * <p>This package manages the lifecycle of Apache Arrow {@code VectorSchemaRoot} instances
 * used as in-memory columnar batches before they are exported to the native Parquet writer.
 * Each VSR follows a strict state machine: {@code ACTIVE → FROZEN → CLOSED}.
 *
 * <h2>Key Classes</h2>
 * <ul>
 *   <li>{@link org.opensearch.parquet.vsr.VSRState} — Enum defining the three lifecycle states.</li>
 *   <li>{@link org.opensearch.parquet.vsr.ManagedVSR} — Wrapper around a single {@code VectorSchemaRoot}
 *       that enforces state transitions and provides Arrow C Data Interface export.</li>
 *   <li>{@link org.opensearch.parquet.vsr.VSRPool} — Manages one ACTIVE and one FROZEN VSR slot,
 *       handling row-count-based rotation when the configurable threshold is reached.</li>
 *   <li>{@link org.opensearch.parquet.vsr.VSRManager} — Top-level orchestrator that combines
 *       the VSR pool with the native Parquet writer, handling document ingestion, VSR rotation,
 *       batch export, and file finalization.</li>
 * </ul>
 *
 * <h2>Rotation Flow</h2>
 * <pre>
 * addDocument() → row count reaches threshold
 *              → active VSR frozen, moved to frozen slot
 *              → frozen VSR exported to native writer via Arrow C Data Interface
 *              → frozen VSR closed and released
 *              → new active VSR created from pool
 * </pre>
 *
 * @see org.opensearch.parquet.vsr.VSRManager
 * @see org.opensearch.parquet.vsr.VSRPool
 * @see org.opensearch.parquet.ParquetSettings#MAX_ROWS_PER_VSR
 */
package org.opensearch.parquet.vsr;
