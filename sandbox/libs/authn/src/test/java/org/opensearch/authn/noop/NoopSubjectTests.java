/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.authn.noop;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.authn.Subject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class NoopSubjectTests extends OpenSearchTestCase {

    public void testAllNoopSubjectAreEquals() {
        assertThat(new NoopSubject(), equalTo(new NoopSubject()));
    }

    public void testHashCode() {
        final Subject s1 = new NoopSubject();
        final Subject s2 = new NoopSubject();
        assertThat(s1.hashCode(), equalTo(s2.hashCode()));
    }

    public void testToString() {
        final Subject s = new NoopSubject();
        assertThat(s.toString(), equalTo("NoopSubject(principal=StringPrincipal(name=Unauthenticated))"));
    }

}
