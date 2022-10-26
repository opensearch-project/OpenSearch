/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.identity;

import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.Principal;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;

public class PrincipalIdentifierTokenTests extends OpenSearchTestCase {

    public void testInstantiationWithStreamInput() throws IOException {
        String extensionUniqueId = "ext_1";
        Principal principal = () -> "user";
        ExtensionTokenProcessor extensionTokenProcessor = new ExtensionTokenProcessor(extensionUniqueId);
        PrincipalIdentifierToken principalIdentifierToken;
        SecretKey secretKey;
        String extractedTokenOne;

        try {
            principalIdentifierToken = extensionTokenProcessor.generateToken(principal);
            secretKey = extensionTokenProcessor.getSecretKey();
            extractedTokenOne = extensionTokenProcessor.extractPrincipal(principalIdentifierToken, secretKey);
        } catch (InvalidKeyException | NoSuchAlgorithmException | NoSuchPaddingException | InvalidAlgorithmParameterException
            | IllegalBlockSizeException | BadPaddingException e) {

            throw new Error(e);
        }

        String expectedToken = principal.getName();
        assertEquals(expectedToken, extractedTokenOne);

        // TODO: Implement this so it compares the same input generating the same token via stream
        // try (BytesStreamOutput out = new BytesStreamOutput()) {
        // principalIdentifierToken.writeTo(out);
        // out.flush();
        // try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {
        // principalIdentifierToken = new PrincipalIdentifierToken(in);
        //
        // assertEquals(expectedToken, principalIdentifierToken.getToken());
        // }
        // }
    }
}
