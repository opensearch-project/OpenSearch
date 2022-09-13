/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.mock;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.equalTo;

public class IdentityTests extends OpenSearchTestCase {

    public void testGetAuthManagerDefaultsToNull() {
        assertThat(Identity.getAuthManager(), nullValue());
    }

    public void testGetAuthManagerSetAndGets() {
        final AuthenticationManager authManager = mock(AuthenticationManager.class);
        Identity.setAuthManager(authManager);

        assertThat(Identity.getAuthManager(), equalTo(authManager));
    }

}
