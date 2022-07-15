/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster;

/**
 * Enables listening to cluster-manager changes events of the local node (when the local node becomes the cluster-manager, and when the local
 * node cease being a cluster-manager).
 *
 * @opensearch.internal
 * @deprecated As of 2.2, because supporting inclusive language, replaced by {@link LocalNodeClusterManagerListener}
 */
@Deprecated
public interface LocalNodeMasterListener extends LocalNodeClusterManagerListener {

}
