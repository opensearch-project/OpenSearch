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
import static org.junit.Assert.assertNotNull;
import static org.opensearch.extensions.action.ExtensionActionUtil.UNIT_SEPARATOR;
import static org.opensearch.extensions.action.ExtensionActionUtil.createProxyRequestBytes;

public class ExtensionActionUtilTest {
    private byte[] expected;
    private final String action = "org.opensearch.action.someactionclass";

    @Before
    public void setUp() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        ExtensionActionRequest actionParams = new ExtensionActionRequest(action, new byte[] {});
        actionParams.writeTo(out);
        out.flush();

        byte[] requestBytes = BytesReference.toBytes(out.bytes());
        byte[] requestClass = ExtensionActionRequest.class.getName().getBytes();
        this.expected = ByteBuffer.allocate(requestClass.length + 1 + requestBytes.length)
            .put(requestClass)
            .put(UNIT_SEPARATOR)
            .put(requestBytes)
            .array();
    }

    @Test
    public void testCreateProxyRequestBytes() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        ExtensionActionRequest actionParams = new ExtensionActionRequest(action, new byte[] {});
        actionParams.writeTo(out);
        out.flush();

        byte[] requestBytes = BytesReference.toBytes(out.bytes());
        ExtensionActionRequest request = new ExtensionActionRequest(action, requestBytes);
        byte[] result = createProxyRequestBytes(request);
        assertArrayEquals(expected, result);
    }

    @Test
    public void testCreateExtensionActionRequestFromBytes() {
        ExtensionActionRequest extensionActionRequest = ExtensionActionUtil.createExtensionActionRequestFromBytes(expected);
        assertNotNull(extensionActionRequest);
    }

    @Test
    public void testCreateActionRequest() {
        ExtensionActionRequest request = new ExtensionActionRequest(action, expected);
        ActionRequest actionRequest = ExtensionActionUtil.createActionRequest(request);
        assertNotNull(actionRequest);
    }

    @Test
    public void testCreateExtensionActionRequest() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        MyExampleRequest request = new MyExampleRequest(action, new byte[] {});
        request.writeTo(out);
        out.flush();

        byte[] requestBytes = BytesReference.toBytes(out.bytes());
        byte[] requestClass = MyExampleRequest.class.getName().getBytes(StandardCharsets.UTF_8);
        byte[] result = ByteBuffer.allocate(requestClass.length + 1 + action.codePointAt(0))
            .put(requestClass)
            .put(UNIT_SEPARATOR)
            .put(requestBytes)
            .array();

        MyExampleRequest myExampleRequest = new MyExampleRequest(StreamInput.wrap(result));
        ExtensionActionRequest extensionActionRequest = ExtensionActionUtil.createExtensionActionRequest(myExampleRequest);
        assertNotNull(extensionActionRequest);
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
            this.action = in.readString();
            this.requestBytes = in.readByteArray();
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
