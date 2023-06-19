/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.shiro;

import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import org.junit.Before;
import org.junit.After;

import java.security.Principal;

import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.equalTo;

public class ShiroSubjectTests extends OpenSearchTestCase {

    private org.apache.shiro.subject.Subject shiroSubject;
    private ShiroTokenManager authTokenHandler;
    private ShiroSubject subject;

    @Before
    public void setup() {
        shiroSubject = mock(org.apache.shiro.subject.Subject.class);
        authTokenHandler = mock(ShiroTokenManager.class);
        subject = new ShiroSubject(authTokenHandler, shiroSubject);
    }

    @After
    public void cleanup() {
        verifyNoMoreInteractions(shiroSubject);
    }

    public void testGetPrincipal_null() {
        when(shiroSubject.getPrincipal()).thenReturn(null);

        final Principal result = subject.getPrincipal();

        assertThat(result, nullValue());
        verify(shiroSubject).getPrincipal();
    }

    public void testGetPrincipal_principal() {
        final Principal mockPrincipal = mock(Principal.class);
        when(shiroSubject.getPrincipal()).thenReturn(mockPrincipal);

        final Principal result = subject.getPrincipal();

        assertThat(result, equalTo(mockPrincipal));
        verify(shiroSubject).getPrincipal();
    }

    public void testGetPrincipal_otherType() {
        final Object objPrincipal = mock(Object.class);
        when(shiroSubject.getPrincipal()).thenReturn(objPrincipal);
        when(objPrincipal.toString()).thenReturn("objectPrincipalString");

        final Principal result = subject.getPrincipal();

        // assertThat(result, equalTo("objectPrincipalString"));
        verify(shiroSubject).getPrincipal();
        verifyNoMoreInteractions(objPrincipal);
    }

}
