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

    public void testAcknowledgedResponse() throws Exception {
        boolean response = true;
        AcknowledgedResponse booleanResponse = new AcknowledgedResponse(response);

        assertEquals(response, booleanResponse.getStatus());

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            booleanResponse.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {
                booleanResponse = new AcknowledgedResponse(in);

                assertEquals(response, booleanResponse.getStatus());
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
