/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions.rest;

import java.util.List;

import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.BytesStreamInput;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.test.OpenSearchTestCase;

public class RegisterRestActionsTests extends OpenSearchTestCase {

    public void testRegisterRestActionsRequest() throws Exception {
        String uniqueIdStr = "uniqueid1";
        List<String> expected = List.of("GET /foo", "PUT /bar", "POST /baz");
        RegisterRestActionsRequest registerRestActionsRequest = new RegisterRestActionsRequest(uniqueIdStr, expected);

        assertEquals(uniqueIdStr, registerRestActionsRequest.getUniqueId());
        List<String> restActions = registerRestActionsRequest.getRestActions();
        assertEquals(expected.size(), restActions.size());
        assertTrue(restActions.containsAll(expected));
        assertTrue(expected.containsAll(restActions));

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            registerRestActionsRequest.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {
                registerRestActionsRequest = new RegisterRestActionsRequest(in);

                assertEquals(uniqueIdStr, registerRestActionsRequest.getUniqueId());
                restActions = registerRestActionsRequest.getRestActions();
                assertEquals(expected.size(), restActions.size());
                assertTrue(restActions.containsAll(expected));
                assertTrue(expected.containsAll(restActions));
            }
        }
    }
}
