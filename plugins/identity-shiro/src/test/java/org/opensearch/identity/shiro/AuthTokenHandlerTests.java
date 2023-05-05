/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.shiro;

import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.UsernamePasswordToken;
import org.opensearch.identity.tokens.AuthToken;
import org.opensearch.identity.tokens.BasicAuthToken;
import org.opensearch.identity.tokens.NoopToken;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

import java.util.Optional;

public class AuthTokenHandlerTests extends OpenSearchTestCase {

    private ShiroTokenHandler authTokenHandler;

    @Before
    public void testSetup() {
        authTokenHandler = new ShiroTokenHandler();
    }

    public void testShouldExtractBasicAuthTokenSuccessfully() {
        final BasicAuthToken authToken = new BasicAuthToken("Basic YWRtaW46YWRtaW4="); // admin:admin

        final AuthenticationToken translatedToken = authTokenHandler.translateAuthToken(authToken).get();
        assertThat(translatedToken, is(instanceOf(UsernamePasswordToken.class)));

        final UsernamePasswordToken usernamePasswordToken = (UsernamePasswordToken) translatedToken;
        assertThat(usernamePasswordToken, notNullValue());
        assertThat(usernamePasswordToken.getUsername(), equalTo("admin"));
        assertThat(new String(usernamePasswordToken.getPassword()), equalTo("admin"));
    }

    public void testShouldExtractBasicAuthTokenSuccessfully_twoSemiColonPassword() {
        final BasicAuthToken authToken = new BasicAuthToken("Basic dGVzdDp0ZTpzdA=="); // test:te:st

        final AuthenticationToken translatedToken = authTokenHandler.translateAuthToken(authToken).get();
        assertThat(translatedToken, is(instanceOf(UsernamePasswordToken.class)));

        final UsernamePasswordToken usernamePasswordToken = (UsernamePasswordToken) translatedToken;
        assertThat(usernamePasswordToken, notNullValue());
        assertThat(usernamePasswordToken.getUsername(), equalTo("test"));
        assertThat(new String(usernamePasswordToken.getPassword()), equalTo("te:st"));
    }

    public void testShouldReturnNullWhenExtractingNullToken() {
        final Optional<AuthenticationToken> translatedToken = authTokenHandler.translateAuthToken(null);

        assertThat(translatedToken.isEmpty(), is(true));
    }

    public void testShouldRevokeTokenSuccessfully() {
        final BasicAuthToken authToken = new BasicAuthToken("Basic dGVzdDp0ZTpzdA==");
        assertTrue(authToken.toString().equals("Basic auth token with user=test, password=te:st"));
        authTokenHandler.revokeToken(authToken);
        assert(authToken.toString().equals("Basic auth token with user=, password="));
    }

    public void testShouldFailWhenRevokeToken() {
        final NoopToken authToken = new NoopToken();
        assert(authToken.getTokenIdentifier().equals("Noop"));
        assertThrows(UnsupportedAuthenticationToken.class, () -> authTokenHandler.revokeToken(authToken));
    }

    public void testShouldGetTokenInfoSuccessfully() {
        final BasicAuthToken authToken = new BasicAuthToken("Basic dGVzdDp0ZTpzdA==");
        assert(authToken.toString().equals(authTokenHandler.getTokenInfo(authToken)));
    }

    public void testShouldFailGetTokenInfo() {
        final NoopToken authToken = new NoopToken();
        assert(authToken.getTokenIdentifier().equals("Noop"));
        assertThrows(UnsupportedAuthenticationToken.class, () -> authTokenHandler.getTokenInfo(authToken));
    }


    public void testShouldFailValidateToken() {
        final AuthToken authToken = new NoopToken();
        assertFalse(authTokenHandler.validateToken(authToken));
    }
}
