/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.shiro;

import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.UsernamePasswordToken;
import org.opensearch.common.Randomness;
import org.opensearch.identity.IdentityService;
import org.opensearch.identity.tokens.AuthToken;
import org.opensearch.identity.tokens.BasicAuthToken;
import org.opensearch.identity.tokens.TokenManager;
import org.passay.CharacterRule;
import org.passay.EnglishCharacterData;
import org.passay.PasswordGenerator;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Extracts Shiro's {@link AuthenticationToken} from different types of auth headers
 *
 * @opensearch.experimental
 */
class ShiroTokenManager implements TokenManager {

    private static final Logger log = LogManager.getLogger(IdentityService.class);

    private static Map<BasicAuthToken, String> shiroTokenPasswordMap = new HashMap<>();

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
    public AuthToken issueToken(String audience) {

        String password = generatePassword();
        final byte[] rawEncoded = Base64.getEncoder().encode((audience + ":" + password).getBytes(UTF_8));
        final String usernamePassword = new String(rawEncoded, UTF_8);
        final String header = "Basic " + usernamePassword;
        BasicAuthToken token = new BasicAuthToken(header);
        shiroTokenPasswordMap.put(token, password);

        return token;
    }

    public boolean validateToken(AuthToken token) {
        if (token instanceof BasicAuthToken) {
            final BasicAuthToken basicAuthToken = (BasicAuthToken) token;
            return basicAuthToken.getUser().equals(SecurityUtils.getSubject().toString())
                && basicAuthToken.getPassword().equals(shiroTokenPasswordMap.get(basicAuthToken));
        }
        return false;
    }

    public String getTokenInfo(AuthToken token) {
        if (token instanceof BasicAuthToken) {
            final BasicAuthToken basicAuthToken = (BasicAuthToken) token;
            return basicAuthToken.toString();
        }
        throw new UnsupportedAuthenticationToken();
    }

    public void revokeToken(AuthToken token) {
        if (token instanceof BasicAuthToken) {
            final BasicAuthToken basicAuthToken = (BasicAuthToken) token;
            basicAuthToken.revoke();
            return;
        }
        throw new UnsupportedAuthenticationToken();
    }

    public void resetToken(AuthToken token) {
        if (token instanceof BasicAuthToken) {
            final BasicAuthToken basicAuthToken = (BasicAuthToken) token;
            basicAuthToken.revoke();
        }
    }

    /**
     * When the ShiroTokenManager is in use, a random password is generated for each token and is then output to the logs.
     * The password is used for development only.
     * @return A randomly generated password for development
     */
    public String generatePassword() {

        CharacterRule lowercaseCharacterRule = new CharacterRule(EnglishCharacterData.LowerCase, 1);
        CharacterRule uppercaseCharacterRule = new CharacterRule(EnglishCharacterData.UpperCase, 1);
        CharacterRule numericCharacterRule = new CharacterRule(EnglishCharacterData.Digit, 1);
        CharacterRule specialCharacterRule = new CharacterRule(EnglishCharacterData.Special, 1);

        List<CharacterRule> rules = Arrays.asList(
            lowercaseCharacterRule,
            uppercaseCharacterRule,
            numericCharacterRule,
            specialCharacterRule
        );
        PasswordGenerator passwordGenerator = new PasswordGenerator();

        Random random = Randomness.get();

        String password = passwordGenerator.generatePassword(random.nextInt(8) + 8, rules); // Generate a 8 to 16 char password
        log.info("Generated password: " + password);
        return password;
    }

    Map<BasicAuthToken, String> getShiroTokenPasswordMap() {
        return shiroTokenPasswordMap;
    }

}
