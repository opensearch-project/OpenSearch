/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.shiro;

import java.security.Principal;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.opensearch.action.ActionScope;
import org.opensearch.action.admin.cluster.state.ClusterStateAction;
import org.opensearch.action.admin.indices.shrink.ResizeAction;
import org.opensearch.action.get.GetAction;
import org.opensearch.action.get.MultiGetAction;
import org.opensearch.identity.ApplicationAwareSubject;
import org.opensearch.identity.scopes.Scope;
import org.opensearch.identity.scopes.ScopeEnums;
import org.opensearch.test.OpenSearchTestCase;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class ShiroSubjectTests extends OpenSearchTestCase {

    private org.apache.shiro.subject.Subject shiroSubject;
    private ShiroTokenManager authTokenHandler;
    private ShiroSubject subject;
    private ApplicationAwareSubject applicationAwareSubject;

    @Before
    public void setup() {
        shiroSubject = mock(org.apache.shiro.subject.Subject.class);
        authTokenHandler = mock(ShiroTokenManager.class);
        subject = new ShiroSubject(authTokenHandler, shiroSubject);
        applicationAwareSubject = new ApplicationAwareSubject(subject);
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

    public void testSetAndGetScopesShouldPass() {

        List<Scope> testScopes = List.of(ActionScope.READ);
        // Set scopes for a subject
        subject.setScopes(testScopes);
        assertEquals(subject.getScopes(), testScopes);

        List<Scope> testScopes2 = List.of(ActionScope.ALL);
        subject.setScopes(testScopes2);
        assertEquals(subject.getScopes(), testScopes2);
        assertFalse(subject.getScopes().contains(ActionScope.READ)); // Verify that setScopes overwrites completely
    }

    public void testSetScopeGetActionAreaName() {

        assertEquals(ActionScope.ALL.getAction(), "ALL");
        assertEquals(ActionScope.ALL.getArea(), ScopeEnums.ScopeArea.ALL);
        assertEquals(ActionScope.ALL.getNamespace(), ScopeEnums.ScopeNamespace.ACTION);

        assertEquals(ActionScope.READ.getAction(), "READ");
        assertEquals(ActionScope.READ.getArea(), ScopeEnums.ScopeArea.INDEX);
        assertEquals(ActionScope.READ.getNamespace(), ScopeEnums.ScopeNamespace.ACTION);
    }

    public void testIsAllowedShouldPass() {

        List<Scope> testScopes = List.of(ActionScope.READ);
        // Set scopes for a subject
        subject.setScopes(testScopes);
        assertEquals(subject.getScopes(), testScopes);

        GetAction getAction = GetAction.INSTANCE;
        MultiGetAction multiGetAction = MultiGetAction.INSTANCE;
        assertTrue(applicationAwareSubject.isAllowed(getAction.getAllowedScopes()));
        assertTrue(applicationAwareSubject.isAllowed(multiGetAction.getAllowedScopes()));
    }

    public void testIsAllowedShouldFail() {

        List<Scope> testScopes = List.of(ActionScope.READ);
        // Set scopes for a subject
        subject.setScopes(testScopes);
        assertEquals(subject.getScopes(), testScopes);

        ResizeAction resizeAction = ResizeAction.INSTANCE;
        ClusterStateAction clusterStateAction = ClusterStateAction.INSTANCE;
        assertFalse(applicationAwareSubject.isAllowed(resizeAction.getAllowedScopes()));
        assertFalse(applicationAwareSubject.isAllowed(clusterStateAction.getAllowedScopes()));
    }
}
