/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.identity;

import org.opensearch.authn.AuthenticationManager;
import org.opensearch.authn.Identity;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.mock;
import static org.hamcrest.Matchers.equalTo;

public class IdentityTests extends OpenSearchTestCase {

    public void testGetAuthManagerSetAndGets() {
        final AuthenticationManager authManager = mock(AuthenticationManager.class);
        Identity.setAuthManager(authManager);

        assertThat(Identity.getAuthManager(), equalTo(authManager));
    }

}
