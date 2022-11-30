/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.authn;

import org.apache.shiro.authc.UsernamePasswordToken;
import org.hamcrest.MatcherAssert;
import org.opensearch.authn.tokens.AuthenticationToken;
import org.opensearch.authn.tokens.BasicAuthToken;
import org.opensearch.test.OpenSearchTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class AuthenticationTokenHandlerTests extends OpenSearchTestCase {

    public void testShouldExtractBasicAuthTokenSuccessfully() {

        // The auth header that is part of the request
        String authHeader = "Basic YWRtaW46YWRtaW4="; // admin:admin

        AuthenticationToken authToken = new BasicAuthToken(authHeader);

        UsernamePasswordToken usernamePasswordToken = (UsernamePasswordToken) AuthenticationTokenHandler.extractShiroAuthToken(authToken);

        MatcherAssert.assertThat(usernamePasswordToken, notNullValue());
        MatcherAssert.assertThat(usernamePasswordToken.getUsername(), equalTo("admin"));
        MatcherAssert.assertThat(new String(usernamePasswordToken.getPassword()), equalTo("admin"));
    }

    public void testShouldExtractBasicAuthTokenSuccessfully_twoSemiColonPassword() {

        // The auth header that is part of the request
        String authHeader = "Basic dGVzdDp0ZTpzdA=="; // test:te:st

        AuthenticationToken authToken = new BasicAuthToken(authHeader);

        UsernamePasswordToken usernamePasswordToken = (UsernamePasswordToken) AuthenticationTokenHandler.extractShiroAuthToken(authToken);

        MatcherAssert.assertThat(usernamePasswordToken, notNullValue());
        MatcherAssert.assertThat(usernamePasswordToken.getUsername(), equalTo("test"));
        MatcherAssert.assertThat(new String(usernamePasswordToken.getPassword()), equalTo("te:st"));
    }

    public void testShouldReturnNullWhenExtractingInvalidToken() {
        String authHeader = "Basic Nah";

        AuthenticationToken authToken = new BasicAuthToken(authHeader);

        UsernamePasswordToken usernamePasswordToken = (UsernamePasswordToken) AuthenticationTokenHandler.extractShiroAuthToken(authToken);

        MatcherAssert.assertThat(usernamePasswordToken, nullValue());
    }

    public void testShouldReturnNullWhenExtractingNullToken() {

        org.apache.shiro.authc.AuthenticationToken shiroAuthToken = AuthenticationTokenHandler.extractShiroAuthToken(null);

        MatcherAssert.assertThat(shiroAuthToken, nullValue());
    }
}
