/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.authn;

import org.opensearch.authn.StringPrincipal;
import org.opensearch.test.OpenSearchTestCase;

import java.security.Principal;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class StringPrincipalTests extends OpenSearchTestCase {

    public void testSamePrincipal() {
        final Principal p1 = new StringPrincipal("p1");

        assertThat(p1, equalTo(p1));
        assertThat(p1.hashCode(), equalTo(p1.hashCode()));
        assertThat(p1.toString(), equalTo(p1.toString()));
    }

    public void testDifferentStringPrincipalAreDiffer() {
        final Principal p1 = new StringPrincipal("p1");
        final Principal p2 = new StringPrincipal("p2");

        assertThat(p1, not(equalTo(p2)));
        assertThat(p1.hashCode(), not(equalTo(p2.hashCode())));
        assertThat(p1.toString(), not(equalTo(p2.toString())));
    }

    public void testHashCode() {
        final Principal p1 = new StringPrincipal("p1");
        final Principal p1Duplicated = new StringPrincipal("p1");
        assertThat(p1.hashCode(), equalTo(p1Duplicated.hashCode()));
    }

    public void testToString() {
        final Principal p1 = new StringPrincipal("p1");
        assertThat(p1.toString(), equalTo("StringPrincipal(name=p1)"));
    }

}
