/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.shiro;

import java.util.Base64;
import java.util.Optional;

import org.apache.shiro.SecurityUtils;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.UsernamePasswordToken;
import org.opensearch.identity.Subject;
import org.opensearch.identity.tokens.AuthToken;
import org.opensearch.identity.tokens.BasicAuthToken;
import org.opensearch.identity.tokens.TokenManager;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Extracts Shiro's {@link AuthenticationToken} from different types of auth headers
 *
 * @opensearch.experimental
 */
class ShiroTokenHandler implements TokenManager {

    /**
     * Translates into shiro auth token from the given header token
     * @param authenticationToken the token from which to translate
     * @return An optional of the shiro auth token for login
     */
    public Optional<AuthenticationToken> translateAuthToken(org.opensearch.identity.tokens.AuthToken authenticationToken) {
        if (authenticationToken instanceof BasicAuthToken) {
            final BasicAuthToken basicAuthToken = (BasicAuthToken) authenticationToken;
            return Optional.of(new UsernamePasswordToken(basicAuthToken.getUser(), basicAuthToken.getPassword()));
        }

        return Optional.empty();
    }

    @Override
    public AuthToken generateToken() {

        Subject subject = new ShiroSubject(this, SecurityUtils.getSubject());
        final byte[] rawEncoded = Base64.getEncoder().encode((subject.getPrincipal().getName() + ":" + generatePassword()).getBytes(UTF_8));
        final String usernamePassword = new String(rawEncoded, UTF_8);
        final String header = "Basic " + usernamePassword;

        return new BasicAuthToken(header);
    }

    @Override
    public boolean validateToken(AuthToken token) {
        if (token instanceof BasicAuthToken) {
            final BasicAuthToken basicAuthToken = (BasicAuthToken) token;
            if (basicAuthToken.getUser().equals(SecurityUtils.getSubject()) && basicAuthToken.getPassword().equals(generatePassword())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String getTokenInfo(AuthToken token) {
        if (token instanceof BasicAuthToken) {
            final BasicAuthToken basicAuthToken = (BasicAuthToken) token;
            return basicAuthToken.toString();
        }
        throw new UnsupportedAuthenticationToken();
    }

    @Override
    public void revokeToken(AuthToken token) {
        if (token instanceof BasicAuthToken) {
            final BasicAuthToken basicAuthToken = (BasicAuthToken) token;
            basicAuthToken.revoke();
            return;
        }
        throw new UnsupportedAuthenticationToken();
    }

    @Override
    public void resetToken(AuthToken token) {
        if (token instanceof BasicAuthToken) {
            final BasicAuthToken basicAuthToken = (BasicAuthToken) token;
            basicAuthToken.revoke();
        }
    }

    public String generatePassword() {
        return "superSecurePassword1!";
    }

}
