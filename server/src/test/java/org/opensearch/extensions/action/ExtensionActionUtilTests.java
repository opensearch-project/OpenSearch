/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions.action;

import org.junit.Before;
import org.mockito.Mockito;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.opensearch.extensions.action.ExtensionActionUtil.UNIT_SEPARATOR;
import static org.opensearch.extensions.action.ExtensionActionUtil.createProxyRequestBytes;

public class ExtensionActionUtilTests extends OpenSearchTestCase {
    private byte[] myBytes;
    private final String actionName = "org.opensearch.action.MyExampleRequest";
    private final byte[] actionNameBytes = MyExampleRequest.class.getName().getBytes(StandardCharsets.UTF_8);

    @Before
    public void setup() throws IOException {
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

    public void testCreateProxyRequestBytes() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        MyExampleRequest exampleRequest = new MyExampleRequest(actionName, actionNameBytes);
        exampleRequest.writeTo(out);

        byte[] result = createProxyRequestBytes(exampleRequest);
        assertArrayEquals(this.myBytes, result);
        assertThrows(RuntimeException.class, () -> ExtensionActionUtil.createProxyRequestBytes(new MyExampleRequest(null, null)));
    }

    public void testCreateActionRequest() throws ReflectiveOperationException {
        ActionRequest actionRequest = ExtensionActionUtil.createActionRequest(myBytes);
        assertThrows(NullPointerException.class, () -> ExtensionActionUtil.createActionRequest(null));
        assertThrows(ReflectiveOperationException.class, () -> ExtensionActionUtil.createActionRequest(actionNameBytes));
        assertNotNull(actionRequest);
        assertFalse(actionRequest.getShouldStoreResult());
    }

    public void testConvertParamsToBytes() throws IOException {
        Writeable mockWriteableObject = Mockito.mock(Writeable.class);
        Mockito.doThrow(new IOException("Test IOException")).when(mockWriteableObject).writeTo(Mockito.any());
        assertThrows(IllegalStateException.class, () -> ExtensionActionUtil.convertParamsToBytes(mockWriteableObject));
    }

    public void testDelimPos() {
        assertTrue(ExtensionActionUtil.delimPos(myBytes) > 0);
        assertTrue(ExtensionActionUtil.delimPos(actionNameBytes) < 0);
        assertEquals(-1, ExtensionActionUtil.delimPos(actionNameBytes));
    }

    private static class MyExampleRequest extends ActionRequest {
        private final String param1;
        private final byte[] param2;

        public MyExampleRequest(String param1, byte[] param2) {
            this.param1 = param1;
            this.param2 = param2;
        }

        public MyExampleRequest(StreamInput in) throws IOException {
            super(in);
            param1 = in.readString();
            param2 = in.readByteArray();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(param1);
            out.writeByteArray(param2);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }
}
