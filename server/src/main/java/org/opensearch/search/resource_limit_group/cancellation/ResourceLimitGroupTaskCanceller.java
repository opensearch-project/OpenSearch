/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.resource_limit_group.cancellation;

/**
 * This class is used to identify and cancel the violating tasks in a resourceLimitGroup
 */
public abstract class ResourceLimitGroupTaskCanceller {
    private CancellableTaskSelector taskSelector;

    public ResourceLimitGroupTaskCanceller(CancellableTaskSelector taskSelector) {
        this.taskSelector = taskSelector;
    }

    public abstract void cancelTasks();
}
