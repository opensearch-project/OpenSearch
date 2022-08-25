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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNot.not;

public class IdentityTests {

    final UUID uuid1 = UUID.randomUUID();
    final UUID uuid2 = UUID.randomUUID();

    final String user1 = "user1";
    final String user2 = "user2";

    final List<String> emptyList = Collections.emptyList();
    final Map<String, String> emptyMap = Collections.emptyMap();

    final Principal principal1 = new Identity(uuid1, user1, emptyList, emptyMap);
    final Principal principal2 = new Identity(uuid2, user2, emptyList, emptyMap);

    public void testGetPrincipalData() {
        assertThat(principal1.getId(), equalTo(uuid1));
        assertThat(principal1.getUserName(), equalTo(user1));
        assertThat(principal1.getSchemas(), equalTo(emptyList));
        assertThat(principal1.getSchemas(), equalTo(emptyMap));
    }

    public void testAnonymousVsKnownIdentity() {

        assertThat(principal1, not(equalTo(Identity.ANONYMOUS_IDENTITY)));
        assertThat(Identity.ANONYMOUS_IDENTITY, not(equalTo(principal1)));
    }

    public void testEquality() {
        assertThat(principal1, not(equalTo(principal2)));
    }

    public void testAnonymousGetId() {
        assertThat(Identity.ANONYMOUS_IDENTITY.getId(), equalTo(new UUID(0L, 0L)));
    }

}
