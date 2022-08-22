/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.admin.cluster.shards.routing.wrr.get.ClusterGetWRRWeightsResponse;
import org.opensearch.action.admin.cluster.shards.routing.wrr.put.ClusterPutWRRWeightsResponse;

import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Map;

import static org.opensearch.test.OpenSearchIntegTestCase.Scope.TEST;

@OpenSearchIntegTestCase.ClusterScope(scope = TEST, supportsDedicatedMasters = true, numDataNodes = 3)
public class WRRRoutingIT extends OpenSearchIntegTestCase {

    public void testUpdateWRRWeights() {

        Map<String, Object> weights = Map.of("a", "1", "b", "1", "c", "0");
        WRRWeight wrrWeight = new WRRWeight("zone", weights);
        ClusterPutWRRWeightsResponse response = client().admin().cluster().prepareWRRWeights().setWRRWeights(wrrWeight).get();
        assertEquals(response.isAcknowledged(), true);
    }

    public void testUpdateWRRWeights_MoreThanOneZoneHasZeroWeight() {

        Map<String, Object> weights = Map.of("a", "1", "b", "0", "c", "0");
        WRRWeight wrrWeight = new WRRWeight("zone", weights);
        assertThrows(
            ActionRequestValidationException.class,
            () -> client().admin().cluster().prepareWRRWeights().setWRRWeights(wrrWeight).get()
        );
    }

    public void testGetWRRWeights() {

        Map<String, Object> weights = Map.of("a", "1", "b", "1", "c", "1");
        WRRWeight wrrWeight = new WRRWeight("zone", weights);
        ClusterPutWRRWeightsResponse response = client().admin().cluster().prepareWRRWeights().setWRRWeights(wrrWeight).get();
        ClusterGetWRRWeightsResponse response1 = client().admin().cluster().prepareGetWRRWeights().get();
        assertEquals(response1.weights(), wrrWeight);
    }
}
