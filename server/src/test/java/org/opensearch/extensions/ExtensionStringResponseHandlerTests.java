/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions;

import org.junit.Before;
import org.mockito.Mockito;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

public class ExtensionStringResponseHandlerTests extends OpenSearchTestCase {

    private ExtensionStringResponseHandler extensionStringResponseHandler;

    @Before
    public void setup() throws Exception {
        extensionStringResponseHandler = new ExtensionStringResponseHandler("Sample-string");
    }

    public void testReadResponse() throws Exception {
        String expected = "Sample-string";
        StreamInput in = Mockito.mock(StreamInput.class);
        Mockito.when(in.readString()).thenReturn("Sample-string");
        ExtensionStringResponse extensionStringResponse = extensionStringResponseHandler.read(in);
        assertEquals(expected, extensionStringResponse.getResponse());

    }

    public void testHandleResponse() throws Exception {
        String expected = "Sample-string";
        ExtensionStringResponse response = new ExtensionStringResponse("Sample-string");
        extensionStringResponseHandler.handleResponse(response);
        assertEquals(expected, response.getResponse());
    }

    public void testExecutor() {
        String expected = "generic";
        String response = extensionStringResponseHandler.executor();
        assertEquals(expected, response);
    }
}
