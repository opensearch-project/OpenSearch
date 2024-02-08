/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.state.term;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.is;

public class ClusterTermVersionTests extends OpenSearchSingleNodeTestCase {

    public void testTransportTermResponse() throws ExecutionException, InterruptedException {
        GetTermVersionRequest request = new GetTermVersionRequest();
        GetTermVersionResponse resp = client().execute(GetTermVersionAction.INSTANCE, request).get();

        final ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        final ClusterState clusterState = clusterService.state();

        assertThat(resp.getTerm(), is(clusterState.term()));
        assertThat(resp.getVersion(), is(clusterState.version()));
        assertThat(resp.getStateUUID(), is(clusterState.stateUUID()));
        assertThat(resp.getClusterName().value().startsWith("single-node-cluster"), is(true));
    }
}
