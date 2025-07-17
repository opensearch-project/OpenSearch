/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.javaagent;

import org.junit.Test;

import javax.security.auth.Subject;

import java.security.AccessController;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class SubjectTests {
    @SuppressWarnings("removal")
    @Test
    public void testGetSubject() throws Exception {
        assertThat(Subject.getSubject(AccessController.getContext()), is(nullValue()));
    }

    @SuppressWarnings("removal")
    @Test
    public void testCallWithSubject() throws Exception {
        Subject.callAs(new Subject(), () -> {
            assertThat(Subject.getSubject(AccessController.getContext()), is(not(nullValue())));
            return null;
        });
    }
}
