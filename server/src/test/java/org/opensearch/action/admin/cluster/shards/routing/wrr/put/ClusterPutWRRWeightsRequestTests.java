/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.shards.routing.wrr.put;

import org.opensearch.cluster.routing.WRRWeight;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

public class ClusterPutWRRWeightsRequestTests extends OpenSearchTestCase {

    public void testSetWRRWeight() {
        String reqString = "{\"us-east-1c\" : \"0\", \"us-east-1b\":\"1\",\"us-east-1a\":\"1\"}";
        ClusterPutWRRWeightsRequest request = new ClusterPutWRRWeightsRequest();
        Map<String, Object> weights = Map.of("us-east-1a", "1", "us-east-1b", "1", "us-east-1c", "0");
        WRRWeight wrrWeight = new WRRWeight("zone", weights);
        request.attributeName("zone");
        request = request.setWRRWeight(new BytesArray(reqString), XContentType.JSON);
        assertEquals(request.wrrWeight(), wrrWeight);
    }

}
