/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.core.tasks.resourcetracker;

import org.opensearch.common.annotation.PublicApi;

/**
 * Information about resource usage
 *
 *  @opensearch.api
 */
@PublicApi(since = "2.1.0")
public class ResourceUsageMetric {
    private final ResourceStats stats;
    private final long value;

    public ResourceUsageMetric(ResourceStats stats, long value) {
        this.stats = stats;
        this.value = value;
    }

    public ResourceStats getStats() {
        return stats;
    }

    public long getValue() {
        return value;
    }
}
