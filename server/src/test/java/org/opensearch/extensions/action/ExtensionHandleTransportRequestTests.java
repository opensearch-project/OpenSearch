/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions.action;

import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.BytesStreamInput;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;

public class ExtensionHandleTransportRequestTests extends OpenSearchTestCase {
    public void testExtensionHandleTransportRequest() throws Exception {
        String action = "test-action";
        byte[] requestBytes = "request-bytes".getBytes(StandardCharsets.UTF_8);
        ExtensionHandleTransportRequest request = new ExtensionHandleTransportRequest(action, requestBytes);

        assertEquals(action, request.getAction());
        assertEquals(requestBytes, request.getRequestBytes());

        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()));
        request = new ExtensionHandleTransportRequest(in);

        assertEquals(action, request.getAction());
        assertEquals(new String(requestBytes, StandardCharsets.UTF_8), new String(request.getRequestBytes(), StandardCharsets.UTF_8));
    }
}
