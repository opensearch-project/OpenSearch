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
import org.opensearch.OpenSearchException;
import org.opensearch.identity.noop.NoopTokenHandler;
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

    private ShiroTokenHandler shiroAuthTokenHandler;
    private NoopTokenHandler noopTokenHandler;

    @Before
    public void testSetup() {
        shiroAuthTokenHandler = new ShiroTokenHandler();
        noopTokenHandler = new NoopTokenHandler();
    }

    public void testShouldExtractBasicAuthTokenSuccessfully() {
        final BasicAuthToken authToken = new BasicAuthToken("Basic YWRtaW46YWRtaW4="); // admin:admin

        final AuthenticationToken translatedToken = shiroAuthTokenHandler.translateAuthToken(authToken).get();
        assertThat(translatedToken, is(instanceOf(UsernamePasswordToken.class)));

        final UsernamePasswordToken usernamePasswordToken = (UsernamePasswordToken) translatedToken;
        assertThat(usernamePasswordToken, notNullValue());
        assertThat(usernamePasswordToken.getUsername(), equalTo("admin"));
        assertThat(new String(usernamePasswordToken.getPassword()), equalTo("admin"));
    }

    public void testShouldExtractBasicAuthTokenSuccessfully_twoSemiColonPassword() {
        final BasicAuthToken authToken = new BasicAuthToken("Basic dGVzdDp0ZTpzdA=="); // test:te:st

        final AuthenticationToken translatedToken = shiroAuthTokenHandler.translateAuthToken(authToken).get();
        assertThat(translatedToken, is(instanceOf(UsernamePasswordToken.class)));

        final UsernamePasswordToken usernamePasswordToken = (UsernamePasswordToken) translatedToken;
        assertThat(usernamePasswordToken, notNullValue());
        assertThat(usernamePasswordToken.getUsername(), equalTo("test"));
        assertThat(new String(usernamePasswordToken.getPassword()), equalTo("te:st"));
    }

    public void testShouldReturnNullWhenExtractingNullToken() {
        final Optional<AuthenticationToken> translatedToken = shiroAuthTokenHandler.translateAuthToken(null);

        assertThat(translatedToken.isEmpty(), is(true));
    }

    public void testShouldRevokeTokenSuccessfully() {
        final BasicAuthToken authToken = new BasicAuthToken("Basic dGVzdDp0ZTpzdA==");
        assertTrue(authToken.toString().equals("Basic auth token with user=test, password=te:st"));
        shiroAuthTokenHandler.revokeToken(authToken);
        assert (authToken.toString().equals("Basic auth token with user=, password="));
    }

    public void testShouldFailWhenRevokeToken() {
        final NoopToken authToken = new NoopToken();
        assert (authToken.getTokenIdentifier().equals("Noop"));
        assertThrows(UnsupportedAuthenticationToken.class, () -> shiroAuthTokenHandler.revokeToken(authToken));
    }

    public void testShouldGetTokenInfoSuccessfully() {
        final BasicAuthToken authToken = new BasicAuthToken("Basic dGVzdDp0ZTpzdA==");
        assert (authToken.toString().equals(shiroAuthTokenHandler.getTokenInfo(authToken)));
        final NoopToken noopAuthToken = new NoopToken();
        assert (noopTokenHandler.getTokenInfo(noopAuthToken).equals("Token is NoopToken"));
    }

    public void testShouldFailGetTokenInfo() {
        final NoopToken authToken = new NoopToken();
        assert (authToken.getTokenIdentifier().equals("Noop"));
        assertThrows(UnsupportedAuthenticationToken.class, () -> shiroAuthTokenHandler.getTokenInfo(authToken));
    }

    public void testShouldFailValidateToken() {
        final AuthToken authToken = new NoopToken();
        assertFalse(shiroAuthTokenHandler.validateToken(authToken));
    }

    public void testShouldResetToken(AuthToken token) {
        BasicAuthToken authToken = new BasicAuthToken("Basic dGVzdDp0ZTpzdA==");
        shiroAuthTokenHandler.resetToken(authToken);
        assert (authToken.getPassword().equals(""));
        assert (authToken.getUser().equals(""));
    }

    public void testShouldPassThrough() {
        final NoopToken authToken = new NoopToken();
        noopTokenHandler.resetToken(authToken);
        noopTokenHandler.revokeToken(authToken);
    }

    public void testShouldFailPassThrough() {
        BasicAuthToken authToken = new BasicAuthToken("Basic dGVzdDp0ZTpzdA==");
        assertThrows(OpenSearchException.class, () -> noopTokenHandler.resetToken(authToken));
        assertThrows(OpenSearchException.class, () -> noopTokenHandler.revokeToken(authToken));
    }

}
