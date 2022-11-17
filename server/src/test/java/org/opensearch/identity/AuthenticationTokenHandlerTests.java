/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.identity;

import org.hamcrest.MatcherAssert;
import org.opensearch.authn.AuthenticationToken;
import org.opensearch.authn.HttpHeaderToken;
import org.opensearch.test.OpenSearchTestCase;

import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class AuthenticationTokenHandlerTests extends OpenSearchTestCase {

    public void testShouldExtractBasicAuthTokenSuccessfully() {

        // The auth header that is part of the request
        String authHeader = "Basic YWRtaW46YWRtaW4="; // admin:admin

        AuthenticationToken authToken = new HttpHeaderToken(authHeader);

        org.apache.shiro.authc.AuthenticationToken shiroAuthToken = AuthenticationTokenHandler.extractShiroAuthToken(authToken);

        MatcherAssert.assertThat(shiroAuthToken, notNullValue());
    }

    public void testShouldReturnNullWhenExtractingInvalidToken() {
        String authHeader = "Basic Nah";

        AuthenticationToken authToken = new HttpHeaderToken(authHeader);

        org.apache.shiro.authc.AuthenticationToken shiroAuthToken = AuthenticationTokenHandler.extractShiroAuthToken(authToken);

        MatcherAssert.assertThat(shiroAuthToken, nullValue());
    }

    public void testShouldReturnNullWhenExtractingNullToken() {

        org.apache.shiro.authc.AuthenticationToken shiroAuthToken = AuthenticationTokenHandler.extractShiroAuthToken(null);

        MatcherAssert.assertThat(shiroAuthToken, nullValue());
    }
}
