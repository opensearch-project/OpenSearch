/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.authn;

import org.opensearch.action.admin.cluster.health.ClusterHealthRequest;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.Operator;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.InternalTestCluster;

import java.io.IOException;

import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.opensearch.index.query.QueryBuilders.queryStringQuery;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

@ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class BasicAuthenticationIT extends OpenSearchIntegTestCase {

    public void testStartingAndStoppingNodes() throws IOException {
        logger.info("--> cluster has [{}] nodes", internalCluster().size());
        if (internalCluster().size() < 5) {
            final int nodesToStart = randomIntBetween(Math.max(2, internalCluster().size() + 1), 5);
            logger.info("--> growing to [{}] nodes", nodesToStart);
            internalCluster().startNodes(nodesToStart);
        }
        ensureGreen();

        while (internalCluster().size() > 1) {
            final int nodesToRemain = randomIntBetween(1, internalCluster().size() - 1);
            logger.info("--> reducing to [{}] nodes", nodesToRemain);
            internalCluster().ensureAtMostNumDataNodes(nodesToRemain);
            assertThat(internalCluster().size(), lessThanOrEqualTo(nodesToRemain));
        }

        ensureGreen();
    }

    public void testBasicAuth() {
        ClusterHealthRequest request = new ClusterHealthRequest();
        ClusterHealthResponse resp = client().admin().cluster().health(request).actionGet();

        System.out.println("testBasicAuth");
        System.out.println(resp);
    }
}

