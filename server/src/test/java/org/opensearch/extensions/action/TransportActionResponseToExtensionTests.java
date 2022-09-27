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

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class TransportActionResponseToExtensionTests extends OpenSearchTestCase {
    public void testTransportActionRequestToExtension() throws IOException {
        byte[] responseBytes = "response-bytes".getBytes(StandardCharsets.UTF_8);
        TransportActionResponseToExtension response = new TransportActionResponseToExtension(responseBytes);

        assertEquals(responseBytes, response.getResponseBytes());

        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()));
        response = new TransportActionResponseToExtension(in);

        assertEquals(new String(responseBytes, StandardCharsets.UTF_8), new String(response.getResponseBytes(), StandardCharsets.UTF_8));
    }

    public void testSetBytes() {
        byte[] responseBytes = "response-bytes".getBytes(StandardCharsets.UTF_8);
        TransportActionResponseToExtension response = new TransportActionResponseToExtension(new byte[0]);
        assertEquals(new String(new byte[0], StandardCharsets.UTF_8), new String(response.getResponseBytes(), StandardCharsets.UTF_8));

        response.setResponseBytes(responseBytes);
        assertEquals(new String(responseBytes, StandardCharsets.UTF_8), new String(response.getResponseBytes(), StandardCharsets.UTF_8));
    }
}
