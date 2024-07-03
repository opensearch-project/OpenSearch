/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.sandboxing.cancellation;

import org.opensearch.cluster.metadata.Sandbox;
import org.opensearch.search.sandboxing.SandboxLevelResourceUsageView;

import java.util.Set;

// TODO - can be deleted
public class TaskCancellationContext {

    // make this a sandbox level view where each view wil have both list of tasks & resource usage map
    private final Set<SandboxLevelResourceUsageView> sandboxLevelViews;
    private final Set<Sandbox> activeSandboxes;

    public TaskCancellationContext(Set<SandboxLevelResourceUsageView> sandboxLevelViews, Set<Sandbox> activeSandboxes) {
        this.sandboxLevelViews = sandboxLevelViews;
        this.activeSandboxes = activeSandboxes;
    }

    public Set<SandboxLevelResourceUsageView> getSandboxLevelViews() {
        return sandboxLevelViews;
    }

    public Set<Sandbox> getActiveSandboxes() {
        return activeSandboxes;
    }
}
