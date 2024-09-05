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
 * Factory class for {@link ResourceUsageCalculator} singleton implementations
 */
public class ResourceUsageCalculatorFactory {
    private static ResourceUsageCalculatorFactory instance = new ResourceUsageCalculatorFactory();

    private ResourceUsageCalculatorFactory() {}

    public static ResourceUsageCalculatorFactory getInstance() {
        return instance;
    }

    public ResourceUsageCalculator getInstanceForResourceType(ResourceType type) {
        if (type == ResourceType.CPU) {
            return CpuUsageCalculator.getInstance();
        } else if (type == ResourceType.MEMORY) {
            return MemoryUsageCalculator.getInstance();
        }
        throw new IllegalArgumentException(type + " is an invalid resource type");
    }
}
