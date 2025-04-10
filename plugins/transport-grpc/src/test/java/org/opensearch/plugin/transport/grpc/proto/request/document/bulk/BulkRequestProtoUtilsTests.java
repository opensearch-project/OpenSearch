/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.transport.grpc.proto.request.document.bulk;

import org.opensearch.action.support.WriteRequest;
import org.opensearch.protobufs.BulkRequest;
import org.opensearch.protobufs.Refresh;
import org.opensearch.test.OpenSearchTestCase;

import java.text.ParseException;

public class BulkRequestProtoUtilsTests extends OpenSearchTestCase {

    public void testPrepareRequestWithBasicSettings() {
        // Create a protobuf BulkRequest with basic settings
        BulkRequest request = BulkRequest.newBuilder()
            .setIndex("test-index")
            .setRouting("test-routing")
            .setRefresh(Refresh.REFRESH_TRUE)
            .setTimeout("30s")
            .build();

        // Call prepareRequest
        org.opensearch.action.bulk.BulkRequest bulkRequest = BulkRequestProtoUtils.prepareRequest(request);

        // Verify the result
        assertNotNull("BulkRequest should not be null", bulkRequest);
        assertEquals("Refresh policy should match", WriteRequest.RefreshPolicy.IMMEDIATE, bulkRequest.getRefreshPolicy());
    }

    public void testPrepareRequestWithDefaultValues() {
        // Create a protobuf BulkRequest with no specific settings
        BulkRequest request = BulkRequest.newBuilder().build();

        // Call prepareRequest
        org.opensearch.action.bulk.BulkRequest bulkRequest = BulkRequestProtoUtils.prepareRequest(request);

        // Verify the result
        assertNotNull("BulkRequest should not be null", bulkRequest);
        assertEquals("Should have zero requests", 0, bulkRequest.numberOfActions());
        assertEquals("Refresh policy should be null", WriteRequest.RefreshPolicy.NONE, bulkRequest.getRefreshPolicy());
    }

    public void testPrepareRequestWithTimeout() throws ParseException {
        // Create a protobuf BulkRequest with a timeout
        BulkRequest request = BulkRequest.newBuilder().setTimeout("5s").build();

        // Call prepareRequest
        org.opensearch.action.bulk.BulkRequest bulkRequest = BulkRequestProtoUtils.prepareRequest(request);

        // Verify the result
        assertNotNull("BulkRequest should not be null", bulkRequest);
        // The timeout is set in the BulkRequest
        assertEquals("Require alias should be true", "5s", bulkRequest.timeout().toString());

    }
}
