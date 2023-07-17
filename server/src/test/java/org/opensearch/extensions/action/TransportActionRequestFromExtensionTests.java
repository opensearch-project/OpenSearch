/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions.action;

import com.google.protobuf.ByteString;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.BytesStreamInput;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.test.OpenSearchTestCase;

public class TransportActionRequestFromExtensionTests extends OpenSearchTestCase {
    public void testTransportActionRequestFromExtension() throws Exception {
        String expectedAction = "test-action";
        ByteString expectedRequestBytes = ByteString.copyFromUtf8("request-bytes");
        String uniqueId = "test-uniqueId";
        TransportActionRequestFromExtension request = new TransportActionRequestFromExtension(
            expectedAction,
            expectedRequestBytes,
            uniqueId
        );

        assertEquals(expectedAction, request.getAction());
        assertEquals(expectedRequestBytes, request.getRequestBytes());
        assertEquals(uniqueId, request.getUniqueId());

        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()));
        request = new TransportActionRequestFromExtension(in);

        assertEquals(expectedAction, request.getAction());
        assertEquals(expectedRequestBytes, request.getRequestBytes());
        assertEquals(uniqueId, request.getUniqueId());
    }
}
