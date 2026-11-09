/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.document.bulk;

import org.opensearch.action.support.WriteRequest;
import org.opensearch.protobufs.BulkRequest;
import org.opensearch.test.OpenSearchTestCase;

import java.text.ParseException;

public class BulkRequestProtoUtilsTests extends OpenSearchTestCase {

    public void testPrepareRequestWithBasicSettings() {
        // Create a protobuf BulkRequest with basic settings
        BulkRequest request = BulkRequest.newBuilder()
            .setIndex("test-index")
            .setRouting("test-routing")
            .setRefresh(org.opensearch.protobufs.Refresh.REFRESH_TRUE)
            .setTimeout("30s")
            .build();

        // Call prepareRequest
        org.opensearch.action.bulk.BulkRequest bulkRequest = BulkRequestProtoUtils.prepareRequest(request);

        // Verify the result
        assertNotNull("BulkRequest should not be null", bulkRequest);
        assertEquals("Refresh policy should match", WriteRequest.RefreshPolicy.IMMEDIATE, bulkRequest.getRefreshPolicy());
        assertEquals("Timeout should match", "30s", bulkRequest.timeout().toString());
    }

    public void testPrepareRequestWithDefaultValues() {
        // Create a protobuf BulkRequest with no specific settings
        BulkRequest request = BulkRequest.newBuilder().build();

        // Call prepareRequest
        org.opensearch.action.bulk.BulkRequest bulkRequest = BulkRequestProtoUtils.prepareRequest(request);

        // Verify the result
        assertNotNull("BulkRequest should not be null", bulkRequest);
        assertEquals("Should have zero requests", 0, bulkRequest.numberOfActions());
        assertEquals("Refresh policy should be NONE", WriteRequest.RefreshPolicy.NONE, bulkRequest.getRefreshPolicy());
    }

    public void testPrepareRequestWithTimeout() throws ParseException {
        // Create a protobuf BulkRequest with a timeout
        BulkRequest request = BulkRequest.newBuilder().setTimeout("5s").build();

        // Call prepareRequest
        org.opensearch.action.bulk.BulkRequest bulkRequest = BulkRequestProtoUtils.prepareRequest(request);

        // Verify the result
        assertNotNull("BulkRequest should not be null", bulkRequest);
        assertEquals("Timeout should match", "5s", bulkRequest.timeout().toString());
    }

    // TODO: WaitForActiveShards structure changed in protobufs 0.8.0
    /*
    public void testPrepareRequestWithWaitForActiveShards() {
        // Create a WaitForActiveShards with a specific count
        WaitForActiveShards waitForActiveShards = WaitForActiveShards.newBuilder().setCount(2).build();

        // Create a protobuf BulkRequest with wait_for_active_shards
        BulkRequest request = BulkRequest.newBuilder().setWaitForActiveShards(waitForActiveShards).build();

        // Call prepareRequest
        org.opensearch.action.bulk.BulkRequest bulkRequest = BulkRequestProtoUtils.prepareRequest(request);

        // Verify the result
        assertNotNull("BulkRequest should not be null", bulkRequest);
        assertEquals("Wait for active shards should match", ActiveShardCount.from(2), bulkRequest.waitForActiveShards());
    }
    */

    public void testPrepareRequestWithRequireAlias() {
        // Create a protobuf BulkRequest with require_alias set to true
        BulkRequest request = BulkRequest.newBuilder().setRequireAlias(true).build();

        // Call prepareRequest
        org.opensearch.action.bulk.BulkRequest bulkRequest = BulkRequestProtoUtils.prepareRequest(request);

        // Verify the result
        assertNotNull("BulkRequest should not be null", bulkRequest);
        // Note: The BulkRequest doesn't expose a getter for requireAlias, so we can't directly verify it
        // This test mainly ensures that setting requireAlias doesn't cause any exceptions
    }

    public void testPrepareRequestWithPipeline() {
        // Create a protobuf BulkRequest with a pipeline
        BulkRequest request = BulkRequest.newBuilder().setPipeline("test-pipeline").build();

        // Call prepareRequest
        org.opensearch.action.bulk.BulkRequest bulkRequest = BulkRequestProtoUtils.prepareRequest(request);

        // Verify the result
        assertNotNull("BulkRequest should not be null", bulkRequest);
        // Note: The BulkRequest doesn't expose a getter for pipeline, so we can't directly verify it
        // This test mainly ensures that setting pipeline doesn't cause any exceptions
    }

    public void testPrepareRequestWithRefreshWait() {
        // Create a protobuf BulkRequest with refresh set to WAIT_FOR
        BulkRequest request = BulkRequest.newBuilder().setRefresh(org.opensearch.protobufs.Refresh.REFRESH_WAIT_FOR).build();

        // Call prepareRequest
        org.opensearch.action.bulk.BulkRequest bulkRequest = BulkRequestProtoUtils.prepareRequest(request);

        // Verify the result
        assertNotNull("BulkRequest should not be null", bulkRequest);
        assertEquals("Refresh policy should be WAIT_FOR", WriteRequest.RefreshPolicy.WAIT_UNTIL, bulkRequest.getRefreshPolicy());
    }

    public void testPrepareRequestWithRefreshFalse() {
        // Create a protobuf BulkRequest with refresh set to FALSE
        BulkRequest request = BulkRequest.newBuilder().setRefresh(org.opensearch.protobufs.Refresh.REFRESH_FALSE).build();

        // Call prepareRequest
        org.opensearch.action.bulk.BulkRequest bulkRequest = BulkRequestProtoUtils.prepareRequest(request);

        // Verify the result
        assertNotNull("BulkRequest should not be null", bulkRequest);
        assertEquals("Refresh policy should be NONE", WriteRequest.RefreshPolicy.NONE, bulkRequest.getRefreshPolicy());
    }

    public void testPrepareRequestWithTypeThrowsUnsupportedOperationException() {
        // Create a protobuf BulkRequest with deprecated type field
        BulkRequest request = BulkRequest.newBuilder().setType("_doc").build();

        // Call prepareRequest, should throw UnsupportedOperationException
        UnsupportedOperationException exception = expectThrows(
            UnsupportedOperationException.class,
            () -> BulkRequestProtoUtils.prepareRequest(request)
        );

        assertEquals("type param is not supported", exception.getMessage());
    }

    public void testPrepareRequestWithGlobalParamsThrowsUnsupportedOperationException() {
        // Create a protobuf BulkRequest with global_params
        BulkRequest request = BulkRequest.newBuilder().setGlobalParams(org.opensearch.protobufs.GlobalParams.newBuilder().build()).build();

        // Call prepareRequest, should throw UnsupportedOperationException
        UnsupportedOperationException exception = expectThrows(
            UnsupportedOperationException.class,
            () -> BulkRequestProtoUtils.prepareRequest(request)
        );

        assertEquals("global_params param is not supported yet", exception.getMessage());
    }
}
