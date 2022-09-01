/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.identity;

import org.opensearch.test.OpenSearchTestCase;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNot.not;

public class IdentityTests extends OpenSearchTestCase {

    final String user1 = "user1";
    final String user2 = "user2";

    final String id1 = "id1";
    final String id2 = "id2";

    final Principal principal1 = new Identity(id1, user1);
    final Principal principal2 = new Identity(id2, user2);

    public void testGetPrincipalData() {
        assertThat(principal1.getPrincipalIdentifier(), equalTo(id1));
        assertThat(principal1.getUsername(), equalTo(user1));
    }

    public void testAnonymousVsKnownIdentity() {

        assertThat(principal1, not(equalTo(Identity.ANONYMOUS_IDENTITY)));
        assertThat(Identity.ANONYMOUS_IDENTITY, not(equalTo(principal1)));
    }

    public void testEquality() {
        assertThat(principal1, not(equalTo(principal2)));
    }

    public void testAnonymousGetId() {
        assertThat(Identity.ANONYMOUS_IDENTITY.getPrincipalIdentifier(), equalTo(""));
    }
}
