/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions;

import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.BytesStreamInput;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.test.OpenSearchTestCase;

public class ExtensionResponseTests extends OpenSearchTestCase {

    public void testExtensionBooleanResponse() throws Exception {
        boolean response = true;
        ExtensionBooleanResponse booleanRsponse = new ExtensionBooleanResponse(response);

        assertEquals(response, booleanRsponse.getStatus());

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            booleanRsponse.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {
                booleanRsponse = new ExtensionBooleanResponse(in);

                assertEquals(response, booleanRsponse.getStatus());
            }
        }
    }

    public void testExtensionStringResponse() throws Exception {
        String response = "This is a response";
        ExtensionStringResponse stringResponse = new ExtensionStringResponse(response);

        assertEquals(response, stringResponse.getResponse());

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            stringResponse.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {
                stringResponse = new ExtensionStringResponse(in);

                assertEquals(response, stringResponse.getResponse());
            }
        }
    }
}
