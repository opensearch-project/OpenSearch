/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.identity.shiro;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.junit.Before;
import org.opensearch.identity.tokens.AuthToken;
import org.opensearch.identity.tokens.BasicAuthToken;
import org.opensearch.test.OpenSearchTestCase;

public class ShiroServiceAccountManagerTests extends OpenSearchTestCase {

    private ShiroServiceAccountManager shiroServiceAccountManager;

    @Before
    public void setup() {
        shiroServiceAccountManager = new ShiroServiceAccountManager();
    }

    public void testResetServiceAccountTokenPass() {
        String principal = "test";
        AuthToken token = shiroServiceAccountManager.resetServiceAccountToken(principal);
        assert (token instanceof BasicAuthToken);
        BasicAuthToken baseToken = (BasicAuthToken) token;
        String password = baseToken.getPassword();
        assert (password.equals("superSecurePassword1!"));
        assert (baseToken.getUser().equals("test"));
    }

    public void testResetServiceAccountTokenDiffPrincipal() {
        String principal = "notTest";
        AuthToken token = shiroServiceAccountManager.resetServiceAccountToken(principal);
        assert (token instanceof BasicAuthToken);
        BasicAuthToken baseToken = (BasicAuthToken) token;
        String password = baseToken.getPassword();
        assert (password.equals("superSecurePassword1!"));
        assertNotEquals(baseToken.getUser(), "test");
    }

    public void testResetServiceAccountTokenDiffPassword() {
        String principal = "test";
        AuthToken token = shiroServiceAccountManager.resetServiceAccountToken(principal);
        assert (token instanceof BasicAuthToken);
        BasicAuthToken baseToken = (BasicAuthToken) token;
        String password = baseToken.getPassword();
        assertNotEquals(password, "notThePassword2?");
        assert (baseToken.getUser().equals("test"));
    }

    public void testIsValidTokenTrue() {
        String principal = "test";
        AuthToken token = shiroServiceAccountManager.resetServiceAccountToken(principal);
        assertTrue(shiroServiceAccountManager.isValidToken(token));
    }

    public void testIsValidTokenFalse() {
        String principal = "test";
        String encodedString = Base64.getEncoder().encodeToString((principal + ":" + "notThePassword2?").getBytes(StandardCharsets.UTF_8));
        String headerValue = "Basic ".concat(encodedString);
        BasicAuthToken badToken = new BasicAuthToken(headerValue);
        assertFalse(shiroServiceAccountManager.isValidToken(badToken));
    }
}
