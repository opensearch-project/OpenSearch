/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.identity;

import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.BytesStreamInput;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.security.Principal;

public class PrincipalIdentifierTokenTests extends OpenSearchTestCase {

    public void testInstantiationWithStreamInput() throws IOException {
        String extensionUniqueId = "ext_1";
        Principal principal = () -> "user";
        ExtensionTokenProcessor extensionTokenProcessor = new ExtensionTokenProcessor(extensionUniqueId);
        PrincipalIdentifierToken principalIdentifierToken = extensionTokenProcessor.generateToken(principal);

        String expectedToken = principal.getName() + ":" + extensionUniqueId;
        assertEquals(expectedToken, principalIdentifierToken.getToken());

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            principalIdentifierToken.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {
                principalIdentifierToken = new PrincipalIdentifierToken(in);

                assertEquals(expectedToken, principalIdentifierToken.getToken());
            }
        }
    }
}
