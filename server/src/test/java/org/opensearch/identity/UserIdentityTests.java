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

import org.opensearch.identity.UserIdentity.AnonymousUser;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNot.not;

public class UserIdentityTests {

    public void testGetIds() {
        final String idOfSally = "Sally";
        final UserIdentity sally = new UserIdentity(idOfSally);
        
        assertThat(sally.getId(), equalTo(idOfSally));
    }

    public void testUserVsAnonymous() {
        final UserIdentity sally = new UserIdentity("Sally");

        assertThat(sally, not(equalTo(UserIdentity.ANONYMOUS)));
        assertThat(UserIdentity.ANONYMOUS, not(equalTo(sally)));
    }

    public void testEquality() {
        final UserIdentity sally = new UserIdentity("Sally");
        final UserIdentity bob = new UserIdentity("Bob");

        assertThat(sally, not(equalTo(bob)));
    }

    public void testAnonymousGetId() {
        assertThat(UserIdentity.ANONYMOUS.getId(), equalTo(AnonymousUser.ID));
    }

    public void testAnonymousEquality() {
        final Identity blankIdentity = new Identity() {
            @Override
            public String getId() { return null; }
        };
    
        final Identity otherIdentity = new Identity() {
            @Override
            public String getId() { return "other"; }
        };
    
        assertThat(UserIdentity.ANONYMOUS, equalTo(UserIdentity.ANONYMOUS));
        assertThat(UserIdentity.ANONYMOUS, not(equalTo(blankIdentity)));
        assertThat(UserIdentity.ANONYMOUS, not(equalTo(otherIdentity)));
    }
}
