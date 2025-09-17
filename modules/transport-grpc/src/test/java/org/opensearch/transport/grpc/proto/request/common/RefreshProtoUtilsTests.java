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
        Refresh refresh = org.opensearch.protobufs.Refresh.REFRESH_TRUE;

        String refreshPolicy = RefreshProtoUtils.getRefreshPolicy(refresh);

        assertEquals("Should return IMMEDIATE refresh policy", WriteRequest.RefreshPolicy.IMMEDIATE.getValue(), refreshPolicy);
    }

    public void testGetRefreshPolicyWithRefreshWaitFor() {
        Refresh refresh = org.opensearch.protobufs.Refresh.REFRESH_WAIT_FOR;

        String refreshPolicy = RefreshProtoUtils.getRefreshPolicy(refresh);

        assertEquals("Should return WAIT_UNTIL refresh policy", WriteRequest.RefreshPolicy.WAIT_UNTIL.getValue(), refreshPolicy);
    }

    public void testGetRefreshPolicyWithRefreshFalse() {
        Refresh refresh = org.opensearch.protobufs.Refresh.REFRESH_FALSE;

        String refreshPolicy = RefreshProtoUtils.getRefreshPolicy(refresh);

        assertEquals("Should return NONE refresh policy", WriteRequest.RefreshPolicy.NONE.getValue(), refreshPolicy);
    }

    public void testGetRefreshPolicyWithRefreshUnspecified() {
        Refresh refresh = org.opensearch.protobufs.Refresh.UNRECOGNIZED;

        String refreshPolicy = RefreshProtoUtils.getRefreshPolicy(refresh);

        assertEquals("Should return NONE refresh policy", WriteRequest.RefreshPolicy.NONE.getValue(), refreshPolicy);
    }

    public void testGetRefreshPolicyWithNoRefresh() {
        BulkRequest request = BulkRequest.newBuilder().build();

        String refreshPolicy = RefreshProtoUtils.getRefreshPolicy(request.getRefresh());

        assertEquals("Should return NONE refresh policy", WriteRequest.RefreshPolicy.NONE.getValue(), refreshPolicy);
    }

}
