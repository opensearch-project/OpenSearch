/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.shiro;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.opensearch.identity.ServiceAccountManager;
import org.opensearch.identity.tokens.AuthToken;
import org.opensearch.identity.tokens.BasicAuthToken;

public class ShiroServiceAccountManager implements ServiceAccountManager {

    public final static String TOKEN_IDENTIFIER = "Basic";
    @Override
    public AuthToken resetServiceAccountToken(String principal) {
        String generatedPassword = generatePassword();
        String encodedString = Base64.getEncoder().encodeToString((principal + ":" + generatedPassword).getBytes(StandardCharsets.UTF_8));
        String headerValue = TOKEN_IDENTIFIER.concat(encodedString);
        return new BasicAuthToken(headerValue);
    }

    @Override
    public Boolean isValidToken(AuthToken token) {
        if (!(token instanceof BasicAuthToken)) {
            throw new RuntimeException("The Shiro Service Account Manager cannot parse this type of token.");
        }
        BasicAuthToken baseToken = (BasicAuthToken) token;
        String password = baseToken.getPassword();
        if (password.equals(generatePassword())) {
            return true;
        }
        return false;
    }

    @Override
    public void updateServiceAccount(ObjectNode contentAsNode) {

    }

    private String generatePassword() {
        return "superSecurePassword1!";
    }
}
