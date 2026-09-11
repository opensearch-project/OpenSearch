/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Transport action for dynamically loading search plugins at runtime.
 * 
 * <p>This package provides the infrastructure for hot-reloading search plugins
 * without requiring a cluster restart. The main components include:
 * <ul>
 *   <li>{@link org.opensearch.action.admin.cluster.loadsearchplugins.LoadSearchPluginsAction} - The action definition</li>
 *   <li>{@link org.opensearch.action.admin.cluster.loadsearchplugins.LoadSearchPluginsRequest} - Request parameters</li>
 *   <li>{@link org.opensearch.action.admin.cluster.loadsearchplugins.LoadSearchPluginsResponse} - Response with results</li>
 *   <li>{@link org.opensearch.action.admin.cluster.loadsearchplugins.TransportLoadSearchPluginsAction} - Transport handler</li>
 * </ul>
 * 
 * @opensearch.internal
 */
package org.opensearch.action.admin.cluster.loadsearchplugins;
