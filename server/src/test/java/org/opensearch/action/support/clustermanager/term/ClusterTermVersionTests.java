/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support.clustermanager.term;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.util.concurrent.ExecutionException;

public class ClusterTermVersionTests extends OpenSearchSingleNodeTestCase {

    public void testTransportTermResponse() throws ExecutionException, InterruptedException {
        GetTermVersionRequest request = new GetTermVersionRequest();
        GetTermVersionResponse resp = client().execute(GetTermVersionAction.INSTANCE, request).get();

        final ClusterService clusterService = getInstanceFromNode(ClusterService.class);

        assertTrue(resp.matches(clusterService.state()));
    }
}
