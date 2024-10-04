/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

/**
 * This interface should be implemented by all the plugins that define one or more resources.
 *
 * @opensearch.experimental
 */
public interface ResourcePlugin {

    /**
     * Type of the resource
     * @return a string containing the type of the resource
     */
    String getResourceType();

    /**
     * The index where resource meta-data is stored
     * @return the name of the parent index where resource meta-data is stored
     */
    String getResourceIndex();
}
