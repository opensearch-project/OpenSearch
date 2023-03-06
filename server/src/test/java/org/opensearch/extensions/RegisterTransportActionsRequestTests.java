/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions;

import org.junit.Before;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Set;

public class RegisterTransportActionsRequestTests extends OpenSearchTestCase {
    private RegisterTransportActionsRequest originalRequest;

    @Before
    public void setup() {
        this.originalRequest = new RegisterTransportActionsRequest("extension-uniqueId", Set.of("testAction"));
    }

    public void testRegisterTransportActionsRequest() throws IOException {
        BytesStreamOutput output = new BytesStreamOutput();
        originalRequest.writeTo(output);
        StreamInput input = output.bytes().streamInput();
        RegisterTransportActionsRequest parsedRequest = new RegisterTransportActionsRequest(input);
        assertEquals(parsedRequest.getTransportActions(), originalRequest.getTransportActions());
        assertEquals(parsedRequest.getTransportActions().size(), originalRequest.getTransportActions().size());
        assertEquals(parsedRequest.hashCode(), originalRequest.hashCode());
        assertTrue(originalRequest.equals(parsedRequest));
    }

    public void testToString() {
        assertEquals(originalRequest.toString(), "TransportActionsRequest{uniqueId=extension-uniqueId, actions=[testAction]}");
    }
}
