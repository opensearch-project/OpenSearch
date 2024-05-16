/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tasks;

/**
 * Tasks which should be grouped
 */
public interface ResourceLimitGroupTask {
    void setResourceLimitGroupName(String name);

    String getResourceLimitGroupName();
}
