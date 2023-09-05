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

public class ExtensionHandleTransportRequestTests extends OpenSearchTestCase {
    public void testExtensionHandleTransportRequest() throws Exception {
        String expectedAction = "test-action";
        ByteString expectedRequestBytes = ByteString.copyFromUtf8("request-bytes");
        ExtensionHandleTransportRequest request = new ExtensionHandleTransportRequest(expectedAction, expectedRequestBytes);

        assertEquals(expectedAction, request.getAction());
        logger.info(expectedRequestBytes);
        logger.info(request.getRequestBytes());
        assertEquals(expectedRequestBytes, request.getRequestBytes());

        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()));
        request = new ExtensionHandleTransportRequest(in);
        assertEquals(expectedRequestBytes, request.getRequestBytes());
        assertEquals(expectedAction, request.getAction());
    }
}
