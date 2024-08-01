/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm.stats;

import org.opensearch.search.ResourceType;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class will keep the live view of the query group stats
 */
public class QueryGroupState {
    /**
     * completions at the query group level, this is a cumulative counter since the Opensearch start time
     */
    private final AtomicLong completions = new AtomicLong();

    /**
     * rejections at the query group level, this is a cumulative counter since the Opensearch start time
     */
    private final AtomicLong rejections = new AtomicLong();

    /**
     * This is used to store the resource type stat both for CPU and MEMORY
     */
    private final Map<ResourceType, ResourceTypeState> resourceState;

    public QueryGroupState() {
        resourceState = new EnumMap<>(ResourceType.class);
        resourceState.put(ResourceType.CPU, new ResourceTypeState(ResourceType.CPU));
        resourceState.put(ResourceType.MEMORY, new ResourceTypeState(ResourceType.MEMORY));
    }

    public long getCompletions() {
        return completions.get();
    }

    public long getRejections() {
        return rejections.get();
    }

    public Map<ResourceType, ResourceTypeState> getResourceState() {
        return resourceState;
    }

    public void incrementCompletions() {
        completions.incrementAndGet();
    }

    public static class ResourceTypeState {
        private final ResourceType resourceType;
        private final AtomicLong cancellations = new AtomicLong();

        public ResourceTypeState(ResourceType resourceType) {
            this.resourceType = resourceType;
        }

        public long getCancellations() {
            return cancellations.get();
        }

        public void incrementCancellations() {
            cancellations.incrementAndGet();
        }
    }
}
