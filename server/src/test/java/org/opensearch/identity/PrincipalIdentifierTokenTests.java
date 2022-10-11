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
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.Principal;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;

public class PrincipalIdentifierTokenTests extends OpenSearchTestCase {

    public void testInstantiationWithStreamInput() throws IOException {
        String extensionUniqueId = "ext_1";
        Principal principal = () -> "user";
        ExtensionTokenProcessor extensionTokenProcessor = new ExtensionTokenProcessor(extensionUniqueId);
        PrincipalIdentifierToken principalIdentifierToken;
        try {
            principalIdentifierToken = extensionTokenProcessor.generateToken(principal);
        } catch (InvalidKeyException | NoSuchAlgorithmException | NoSuchPaddingException
                | InvalidAlgorithmParameterException | IllegalBlockSizeException | BadPaddingException e) {
            e.printStackTrace();
            throw new Error(e);
        }

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
