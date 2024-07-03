/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.sandboxing.cancellation;

import org.opensearch.search.sandboxing.resourcetype.SystemResource;
import org.opensearch.tasks.Task;

import java.util.Comparator;

public class MostResourceIntensiveTaskFirstStrategy extends AbstractTaskSelectionStrategy {
    private final SystemResource resourceType;

    MostResourceIntensiveTaskFirstStrategy(SystemResource resourceType) {
        this.resourceType = resourceType;
    }

    @Override
    public Comparator<Task> sortingCondition() {
        return Comparator.comparingLong(resourceType::getResourceUsage).reversed();
    }
}
