/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.sandbox.cancellation;

/**
 * This interface is used to identify and cancel the violating tasks in a sandbox
 */
public interface SandboxRequestCanceller {
    /**
     * Cancels the tasks from conteded sandboxes
     */
    void cancelViolatingTasks();
}
