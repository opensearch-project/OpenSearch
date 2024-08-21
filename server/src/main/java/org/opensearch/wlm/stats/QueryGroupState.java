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
 * This class will keep the point in time view of the query group stats
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
     * this will track the cumulative failures in a query group
     */
    private final AtomicLong failures = new AtomicLong();

    /**
     * This is used to store the resource type state both for CPU and MEMORY
     */
    private final Map<ResourceType, ResourceTypeState> resourceState;

    public QueryGroupState() {
        resourceState = new EnumMap<>(ResourceType.class);
        resourceState.put(ResourceType.CPU, new ResourceTypeState(ResourceType.CPU));
        resourceState.put(ResourceType.MEMORY, new ResourceTypeState(ResourceType.MEMORY));
    }

    /**
     *
     * @return completions in the query group
     */
    public long getCompletions() {
        return completions.get();
    }

    /**
     *
     * @return rejections in the query group
     */
    public long getRejections() {
        return rejections.get();
    }

    /**
     *
     * @return failures in the query group
     */
    public long getFailures() {
        return failures.get();
    }

    /**
     * getter for query group resource state
     * @return the query group resource state
     */
    public Map<ResourceType, ResourceTypeState> getResourceState() {
        return resourceState;
    }

    /**
     * this is a call back to increment cancellations for a query group at task level
     */
    public void incrementCompletions() {
        completions.incrementAndGet();
    }


    /**
     * this is a call back to increment rejections for a query group at incoming request
     */
    public void incrementRejections() {
        rejections.incrementAndGet();
    }


    /**
     * this is a call back to increment failures for a query group
     */
    public void incrementFailures() {
        failures.incrementAndGet();
    }

    /**
     * This class holds the resource level stats for the query group
     */
    public static class ResourceTypeState {
        private final ResourceType resourceType;
        private final AtomicLong cancellations = new AtomicLong();

        public ResourceTypeState(ResourceType resourceType) {
            this.resourceType = resourceType;
        }

        /**
         * getter for resource type cancellations
         */
        public long getCancellations() {
            return cancellations.get();
        }

        /**
         * this will be called when a task is cancelled due to this resource
         */
        public void incrementCancellations() {
            cancellations.incrementAndGet();
        }
    }
}
