/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm.tracker;

import org.opensearch.wlm.ResourceType;

/**
 * Factory class for {@link ResourceUsageUtil} implementations
 */
public class ResourceUsageUtilFactory {
    private static ResourceUsageUtilFactory instance = new ResourceUsageUtilFactory();

    private ResourceUsageUtilFactory() {}

    public static ResourceUsageUtilFactory getInstance() {
        return instance;
    }

    public ResourceUsageUtil getInstanceForResourceType(ResourceType type) {
        if (type == ResourceType.CPU) {
            return ResourceUsageUtil.CpuUsageUtil.getInstance();
        } else if (type == ResourceType.MEMORY) {
            return ResourceUsageUtil.MemoryUsageUtil.getInstance();
        }
        throw new IllegalArgumentException(type + " is an invalid resource type");
    }
}
