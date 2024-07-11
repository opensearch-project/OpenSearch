/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.tiering;

import org.opensearch.action.admin.indices.tiering.TieringRequests;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Interface for processing tiering state.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface TieringStateProcessor {

    void process(final ClusterState clusterState, final ClusterService clusterService, final TieringRequests tieringRequests);
}
