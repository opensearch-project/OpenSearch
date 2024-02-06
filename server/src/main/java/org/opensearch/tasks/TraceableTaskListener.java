/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tasks;

import org.opensearch.telemetry.tracing.Span;

/**
 * Task Listener to ensure ending the span properly.
 */
public class TraceableTaskListener implements TaskManager.TaskEventListeners {

    /**
     * End the span on task cancellation
     * @param task task which got cancelled.
     */
    public void onTaskCancelled(CancellableTask task) {
        Span span = task.getSpan();
        if (span != null) {
            span.endSpan();
        }
    }

    /**
     * End the span on task Completion
     * @param task task which got completed.
     */
    public void onTaskCompleted(Task task) {
        Span span = task.getSpan();
        if (span != null) {
            span.endSpan();
        }
    }
}
