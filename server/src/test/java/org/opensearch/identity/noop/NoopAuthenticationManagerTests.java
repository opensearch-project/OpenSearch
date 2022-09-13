/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.noop;

import org.opensearch.identity.AuthenticationManager;
import org.opensearch.identity.Subject;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.mock;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.equalTo;

public class NoopAuthenticationManagerTests extends OpenSearchTestCase {

    public void testGetSubject() {
       assertThat(new NoopAuthenticationManager().getSubject(), not(nullValue()));
    }

    public void testConsistantSubjects() {
        NoopAuthenticationManager authManager = new NoopAuthenticationManager();
        assertThat(authManager.getSubject(), equalTo(authManager.getSubject()));
    }

}
