/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.decommission.awareness;

import org.opensearch.action.admin.cluster.decommission.awareness.delete.DeleteDecommissionStateRequest;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class DeleteDecommissionStateRequestTests extends OpenSearchTestCase {

    public void testSerialization() throws IOException {
        final DeleteDecommissionStateRequest originalRequest = new DeleteDecommissionStateRequest();

        final DeleteDecommissionStateRequest cloneRequest;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            originalRequest.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                cloneRequest = new DeleteDecommissionStateRequest(in);
            }
        }
        assertEquals(cloneRequest.clusterManagerNodeTimeout(), originalRequest.clusterManagerNodeTimeout());
    }
}
