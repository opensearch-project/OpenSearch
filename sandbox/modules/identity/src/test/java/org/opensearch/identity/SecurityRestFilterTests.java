/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

import org.opensearch.authn.tokens.AuthenticationToken;
import org.opensearch.authn.tokens.BasicAuthToken;
import org.opensearch.test.OpenSearchTestCase;

public class SecurityRestFilterTests extends OpenSearchTestCase {

    public void testBasicAuthTokenType() {
        final String authorizationHeader = "Basic YWRtaW46YWRtaW4=";
        AuthenticationToken authToken = SecurityRestFilter.tokenType(authorizationHeader);

        assertTrue(authToken instanceof BasicAuthToken);
    }
}
