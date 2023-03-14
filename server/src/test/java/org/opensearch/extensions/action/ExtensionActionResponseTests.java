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

public class ExtensionActionResponseTests extends OpenSearchTestCase {

    public void testExtensionActionResponse() throws Exception {
        byte[] expectedResponseBytes = "response-bytes".getBytes(StandardCharsets.UTF_8);
        ExtensionActionResponse response = new ExtensionActionResponse(expectedResponseBytes);

        assertEquals(expectedResponseBytes, response.getResponseBytes());

        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()));
        response = new ExtensionActionResponse(in);
        assertArrayEquals(expectedResponseBytes, response.getResponseBytes());
    }
}
