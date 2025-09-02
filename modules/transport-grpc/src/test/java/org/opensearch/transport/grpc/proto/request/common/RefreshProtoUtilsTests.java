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
import org.opensearch.protobufs.Refresh;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.grpc.proto.request.common.RefreshProtoUtils;

public class RefreshProtoUtilsTests extends OpenSearchTestCase {

    public void testGetRefreshPolicyWithRefreshTrue() {
        // Call getRefreshPolicy
        String refreshPolicy = RefreshProtoUtils.getRefreshPolicy(Refresh.REFRESH_TRUE);

        // Verify the result
        assertEquals("Should return IMMEDIATE refresh policy", WriteRequest.RefreshPolicy.IMMEDIATE.getValue(), refreshPolicy);
    }

    public void testGetRefreshPolicyWithRefreshWaitFor() {

        // Call getRefreshPolicy
        String refreshPolicy = RefreshProtoUtils.getRefreshPolicy(Refresh.REFRESH_WAIT_FOR);

        // Verify the result
        assertEquals("Should return WAIT_UNTIL refresh policy", WriteRequest.RefreshPolicy.WAIT_UNTIL.getValue(), refreshPolicy);
    }

    public void testGetRefreshPolicyWithRefreshFalse() {
        // Call getRefreshPolicy
        String refreshPolicy = RefreshProtoUtils.getRefreshPolicy(Refresh.REFRESH_FALSE);

        // Verify the result
        assertEquals("Should return NONE refresh policy", WriteRequest.RefreshPolicy.NONE.getValue(), refreshPolicy);
    }

    public void testGetRefreshPolicyWithRefreshUnspecified() {
        // Call getRefreshPolicy
        String refreshPolicy = RefreshProtoUtils.getRefreshPolicy(Refresh.REFRESH_UNSPECIFIED);

        // Verify the result
        assertEquals("Should return NONE refresh policy", WriteRequest.RefreshPolicy.NONE.getValue(), refreshPolicy);
    }

    public void testGetRefreshPolicyWithNoRefresh() {
        // Create a protobuf BulkRequest with no refresh value
        BulkRequest request = BulkRequest.newBuilder().build();

        // Call getRefreshPolicy
        String refreshPolicy = RefreshProtoUtils.getRefreshPolicy(request.getRefresh());

        // Verify the result
        assertEquals("Should default to REFRESH_UNSPECIFIED", Refresh.REFRESH_UNSPECIFIED, request.getRefresh());
        assertEquals("Should return NONE refresh policy", WriteRequest.RefreshPolicy.NONE.getValue(), refreshPolicy);
    }

}
