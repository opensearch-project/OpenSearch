/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.resource_limit_group;

/**
 * This interface can be implemented by tasks which will be tracked and monitored using {@link org.opensearch.cluster.metadata.ResourceLimitGroup}
 */
public interface ResourceLimitGroupTask {
    public void setResourceLimitGroupName(String sandboxResourceLimitGroup);

    public String getResourceLimitGroupName();
}
