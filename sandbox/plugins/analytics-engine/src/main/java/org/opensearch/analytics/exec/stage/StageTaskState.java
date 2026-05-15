/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

/**
 * Lifecycle of a single {@link StageTask}. Mirrors the stage's own state machine but
 * tracked per-partition so the {@link TaskTracker} / scheduler can reason about partial
 * progress, retry eligibility, and stage readiness.
 *
 * @opensearch.internal
 */
public enum StageTaskState {
    /** Task descriptor created, not yet dispatched. */
    CREATED,
    /** Dispatched to a data node; awaiting first response or completion. */
    RUNNING,
    /** Terminal success — task finished and its output was handed to the downstream sink. */
    FINISHED,
    /** Terminal failure — the task itself errored or its response stream faulted. */
    FAILED,
    /** Terminal cancellation — the task was cancelled by the parent query or stage. */
    CANCELLED;

    public boolean isTerminal() {
        return this == FINISHED || this == FAILED || this == CANCELLED;
    }
}
