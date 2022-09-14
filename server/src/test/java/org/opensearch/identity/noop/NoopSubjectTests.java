/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.noop;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.identity.Subject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class NoopSubjectTests extends OpenSearchTestCase {

    public void testAllNoopSubjectAreEquals() {
        assertThat(new NoopSubject(), equalTo(new NoopSubject()));
    }

    public void testHashCode() {
        final Subject s = new NoopSubject();
        assertThat(s.hashCode(), equalTo(1331011156));
    }

    public void testToString() {
        final Subject s = new NoopSubject();
        assertThat(s.toString(), equalTo("NoopSubject(principal=StringPrincipal(name=Unauthenticated))"));
    }

}
