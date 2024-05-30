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
import org.opensearch.search.sandboxing.resourcetype.SandboxResourceType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DefaultTaskCancellation extends AbstractTaskCancellation {
    public DefaultTaskCancellation(
        TaskSelectionStrategy cancellationStrategy,
        Map<String, SandboxLevelResourceUsageView> sandboxLevelViews,
        Set<Sandbox> activeSandboxes
    ) {
        super(cancellationStrategy, sandboxLevelViews, activeSandboxes);
    }

    /**
     * // TODO
     * This should cover 3 scenarios
     *     - if node not in duress
     *         - pick sandboxes in enforced mode only
     *     - if node in duress
     *         - pick sandboxes in enforced mode
     *         - tasks running in deleted sandboxes with tasks running
     *         - pick sandboxes in enforced mode
     */
    public List<Sandbox> getSandboxesToCancelFrom() {
        final List<Sandbox> sandboxesToCancelFrom = new ArrayList<>();

        for (Sandbox sandbox : this.activeSandboxes) {
            Map<SandboxResourceType, Long> currentResourceUsage = getResourceUsage(sandbox.getId());

            for (Sandbox.ResourceLimit resourceLimit : sandbox.getResourceLimits()) {
                if (isBreachingThreshold(currentResourceUsage, resourceLimit)) {
                    sandboxesToCancelFrom.add(sandbox);
                    break;
                }
            }
        }

        return sandboxesToCancelFrom;
    }

    private boolean isBreachingThreshold(Map<SandboxResourceType, Long> currentResourceUsage, Sandbox.ResourceLimit resourceLimit) {
        return currentResourceUsage.get(resourceLimit.getResourceType()) > resourceLimit.getThreshold();
    }

    private Map<SandboxResourceType, Long> getResourceUsage(String sandboxId) {
        return sandboxLevelViews.get(sandboxId).getResourceUsageData();
    }
}
