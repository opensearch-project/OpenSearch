/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions.action;

import org.junit.Before;
import org.junit.Test;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.opensearch.extensions.action.ExtensionActionUtil.UNIT_SEPARATOR;
import static org.opensearch.extensions.action.ExtensionActionUtil.createProxyRequestBytes;

public class ExtensionActionUtilTest {
    private byte[] myBytes;
    private final String actionName = "org.opensearch.action.MyExampleRequest";
    private final byte[] actionNameBytes = MyExampleRequest.class.getName().getBytes(StandardCharsets.UTF_8);

    @Before
    public void setUp() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        MyExampleRequest exampleRequest = new MyExampleRequest(actionName, actionNameBytes);
        exampleRequest.writeTo(out);

        byte[] requestBytes = BytesReference.toBytes(out.bytes());
        byte[] requestClass = MyExampleRequest.class.getName().getBytes(StandardCharsets.UTF_8);
        this.myBytes = ByteBuffer.allocate(requestClass.length + 1 + requestBytes.length)
            .put(requestClass)
            .put(UNIT_SEPARATOR)
            .put(requestBytes)
            .array();
    }

    @Test
    public void testCreateProxyRequestBytes() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        MyExampleRequest exampleRequest = new MyExampleRequest(actionName, actionNameBytes);
        exampleRequest.writeTo(out);

        byte[] result = createProxyRequestBytes(exampleRequest);
        assertArrayEquals(this.myBytes, result);
    }

    @Test
    public void testCreateExtensionActionRequestFromBytes() {
        ExtensionActionRequest extensionActionRequest = ExtensionActionUtil.createExtensionActionRequestFromBytes(myBytes);
        assert extensionActionRequest != null;
        String action = extensionActionRequest.getAction();
        byte[] bytes = extensionActionRequest.getRequestBytes().toByteArray();

        assertEquals(this.actionName, action);
        assertArrayEquals(actionNameBytes, bytes);
    }

    @Test
    public void testCreateActionRequest() {
        ActionRequest actionRequest = ExtensionActionUtil.createActionRequest(myBytes);
        assertNotNull(actionRequest);
        assertFalse(actionRequest.getShouldStoreResult());
    }

    @Test
    public void testConvertParamsToBytes() {
        MyExampleRequest exampleRequest = new MyExampleRequest(actionName, actionNameBytes);
        byte[] bytes = ExtensionActionUtil.convertParamsToBytes(exampleRequest);

        ExtensionActionRequest request = ExtensionActionUtil.createExtensionActionRequestFromBytes(bytes);
        assert request != null;
        assertEquals(actionName, request.getAction());
        assertArrayEquals(actionNameBytes, request.getRequestBytes().toByteArray());
        assertNull(ExtensionActionUtil.convertParamsToBytes(null));
        assert bytes != null;
        assertTrue(bytes.length > 0);
    }

    @Test
    public void testDelimPos() {
        assertTrue(ExtensionActionUtil.delimPos(myBytes) > 0);
        assertTrue(ExtensionActionUtil.delimPos(actionNameBytes) < 0);
    }

    private static class MyExampleRequest extends ActionRequest {
        private final String action;
        private final byte[] requestBytes;

        public MyExampleRequest(String action, byte[] requestBytes) {
            this.action = action;
            this.requestBytes = requestBytes;
        }

        public MyExampleRequest(StreamInput in) throws IOException {
            super(in);
            action = in.readString();
            requestBytes = in.readByteArray();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(action);
            out.writeByteArray(requestBytes);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }
}
