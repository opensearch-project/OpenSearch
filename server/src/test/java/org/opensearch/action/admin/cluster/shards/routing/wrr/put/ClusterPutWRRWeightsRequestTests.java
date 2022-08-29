/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.shards.routing.wrr.put;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.cluster.routing.WRRWeights;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

public class ClusterPutWRRWeightsRequestTests extends OpenSearchTestCase {

    public void testSetWRRWeight() {
        String reqString = "{\"us-east-1c\" : \"0\", \"us-east-1b\":\"1\",\"us-east-1a\":\"1\"}";
        ClusterPutWRRWeightsRequest request = new ClusterPutWRRWeightsRequest();
        Map<String, Object> weights = Map.of("us-east-1a", "1", "us-east-1b", "1", "us-east-1c", "0");
        WRRWeights wrrWeight = new WRRWeights("zone", weights);
        request.attributeName("zone");
        request.setWRRWeight(new BytesArray(reqString), XContentType.JSON);
        assertEquals(request.wrrWeight(), wrrWeight);
    }

    public void testValidate_ValuesAreProper() {
        String reqString = "{\"us-east-1c\" : \"1\", \"us-east-1b\":\"0\",\"us-east-1a\":\"1\"}";
        ClusterPutWRRWeightsRequest request = new ClusterPutWRRWeightsRequest();
        request.attributeName("zone");
        request.setWRRWeight(new BytesArray(reqString), XContentType.JSON);
        ActionRequestValidationException actionRequestValidationException = request.validate();
        assertNull(actionRequestValidationException);

    }

    public void testValidate_TwoZonesWithZeroWeight() {
        String reqString = "{\"us-east-1c\" : \"0\", \"us-east-1b\":\"0\",\"us-east-1a\":\"1\"}";
        ClusterPutWRRWeightsRequest request = new ClusterPutWRRWeightsRequest();
        request.attributeName("zone");
        request.setWRRWeight(new BytesArray(reqString), XContentType.JSON);
        ActionRequestValidationException actionRequestValidationException = request.validate();
        assertNotNull(actionRequestValidationException);
        assertTrue(actionRequestValidationException.getMessage().contains("More than one value has weight set as 0"));

    }

    public void testValidate_MissingWeights() {
        String reqString = "{}";
        ClusterPutWRRWeightsRequest request = new ClusterPutWRRWeightsRequest();
        request.attributeName("zone");
        request.setWRRWeight(new BytesArray(reqString), XContentType.JSON);
        ActionRequestValidationException actionRequestValidationException = request.validate();
        assertNotNull(actionRequestValidationException);
        assertTrue(actionRequestValidationException.getMessage().contains("Weights are missing"));

    }

    public void testValidate_AttributeMissing() {
        String reqString = "{\"us-east-1c\" : \"0\", \"us-east-1b\":\"1\",\"us-east-1a\":\"1\"}";
        ClusterPutWRRWeightsRequest request = new ClusterPutWRRWeightsRequest();
        request.setWRRWeight(new BytesArray(reqString), XContentType.JSON);
        ActionRequestValidationException actionRequestValidationException = request.validate();
        assertNotNull(actionRequestValidationException);
        assertTrue(actionRequestValidationException.getMessage().contains("Attribute name is missing"));

    }

    public void testValidate_NonIntegerWeights() {
        String reqString = "{\"us-east-1c\" : \"0\", \"us-east-1b\":\"5.6\",\"us-east-1a\":\"1\"}";
        ClusterPutWRRWeightsRequest request = new ClusterPutWRRWeightsRequest();
        request.attributeName("zone");
        request.setWRRWeight(new BytesArray(reqString), XContentType.JSON);
        ActionRequestValidationException actionRequestValidationException = request.validate();
        assertNotNull(actionRequestValidationException);
        assertTrue(actionRequestValidationException.getMessage().contains("Weight is non-integer"));

    }

}
