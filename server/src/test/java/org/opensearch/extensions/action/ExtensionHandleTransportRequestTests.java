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
        String expectedAction = "test-action";
        byte[] expectedRequestBytes = "request-bytes".getBytes(StandardCharsets.UTF_8);
        ExtensionHandleTransportRequest request = new ExtensionHandleTransportRequest(expectedAction, expectedRequestBytes);

        assertEquals(expectedAction, request.getAction());
        assertEquals(expectedRequestBytes, request.getRequestBytes());

        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()));
        request = new ExtensionHandleTransportRequest(in);

        assertEquals(expectedAction, request.getAction());
        assertArrayEquals(expectedRequestBytes, request.getRequestBytes());
    }
}
