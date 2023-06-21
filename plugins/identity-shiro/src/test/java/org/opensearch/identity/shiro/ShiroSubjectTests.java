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
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.Before;
import org.opensearch.action.ActionScope;
import org.opensearch.action.admin.cluster.state.ClusterStateAction;
import org.opensearch.action.admin.indices.shrink.ResizeAction;
import org.opensearch.action.get.GetAction;
import org.opensearch.action.get.MultiGetAction;
import org.opensearch.extensions.ExtensionsManager;
import org.opensearch.identity.ApplicationAwareSubject;
import org.opensearch.identity.NamedPrincipal;
import org.opensearch.identity.scopes.Scope;
import org.opensearch.identity.scopes.ScopeEnums;
import org.opensearch.test.OpenSearchTestCase;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class ShiroSubjectTests extends OpenSearchTestCase {

    private org.apache.shiro.subject.Subject mockedSubject;
    private ShiroTokenManager authTokenHandler;
    private ShiroSubject shiroSubject;
    private ApplicationAwareSubject applicationAwareSubject;
    private ExtensionsManager extensionsManager;

    @Before
    public void setup() {
        mockedSubject = mock(org.apache.shiro.subject.Subject.class);
        authTokenHandler = mock(ShiroTokenManager.class);
        extensionsManager = mock(ExtensionsManager.class);
        shiroSubject = new ShiroSubject(authTokenHandler, mockedSubject);
        applicationAwareSubject = spy(new ApplicationAwareSubject(shiroSubject));
        extensionsManager = mock(ExtensionsManager.class);

        when(mockedSubject.getPrincipal()).thenReturn(new NamedPrincipal("TestPrincipal"));
    }

    public final Function<Principal, Boolean> checkApplicationExists() {

        return new Function<Principal, Boolean>() {
            @Override
            public Boolean apply(Principal principal) {

                return (true);
            }
        };
    }

    public void testGetPrincipal_null() {
        when(mockedSubject.getPrincipal()).thenReturn(null);

        final Principal result = shiroSubject.getPrincipal();

        assertThat(result, nullValue());
        verify(mockedSubject).getPrincipal();
    }

    public void testGetPrincipal_principal() {
        final Principal mockPrincipal = mock(Principal.class);
        when(mockedSubject.getPrincipal()).thenReturn(mockPrincipal);

        final Principal result = shiroSubject.getPrincipal();

        assertThat(result, equalTo(mockPrincipal));
        verify(mockedSubject).getPrincipal();
    }

    public void testGetPrincipal_otherType() {
        final Object objPrincipal = mock(Object.class);
        when(mockedSubject.getPrincipal()).thenReturn(objPrincipal);
        when(objPrincipal.toString()).thenReturn("objectPrincipalString");

        final Principal result = shiroSubject.getPrincipal();

        // assertThat(result, equalTo("objectPrincipalString"));
        verify(mockedSubject).getPrincipal();
        verifyNoMoreInteractions(objPrincipal);
    }

    public void testSetAndGetScopesShouldPass() {

        List<Scope> testScopes = List.of(ActionScope.READ);
        // Set scopes for ashiroSubject
        shiroSubject.setScopes(testScopes);
        assertEquals(shiroSubject.getScopes(), testScopes);

        List<Scope> testScopes2 = List.of(ActionScope.ALL);
        shiroSubject.setScopes(testScopes2);
        assertEquals(shiroSubject.getScopes(), testScopes2);
        assertFalse(shiroSubject.getScopes().contains(ActionScope.READ)); // Verify that setScopes overwrites completely
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

        List<Scope> allowedScopes = List.of(ActionScope.READ);
        shiroSubject.setScopes(allowedScopes);

        doReturn(true).when(applicationAwareSubject).checkApplicationExists(any());
        doReturn(allowedScopes.stream().map(Scope::asPermissionString).collect(Collectors.toSet())).when(applicationAwareSubject)
            .checkApplicationScopes(any());

        assertTrue(applicationAwareSubject.isAllowed(allowedScopes));

        GetAction getAction = GetAction.INSTANCE;
        MultiGetAction multiGetAction = MultiGetAction.INSTANCE;
        assertTrue(applicationAwareSubject.isAllowed(getAction.getAllowedScopes()));
        assertTrue(applicationAwareSubject.isAllowed(multiGetAction.getAllowedScopes()));
    }

    public void testIsAllowedShouldFail() {

        List<Scope> allowedScopes = List.of(ActionScope.READ);
        shiroSubject.setScopes(allowedScopes);

        doReturn(true).when(applicationAwareSubject).checkApplicationExists(any());
        doReturn(allowedScopes.stream().map(Scope::asPermissionString).collect(Collectors.toSet())).when(applicationAwareSubject)
            .checkApplicationScopes(any());

        assertTrue(applicationAwareSubject.isAllowed(allowedScopes));

        ResizeAction resizeAction = ResizeAction.INSTANCE;
        ClusterStateAction clusterStateAction = ClusterStateAction.INSTANCE;
        assertFalse(applicationAwareSubject.isAllowed(resizeAction.getAllowedScopes()));
        assertFalse(applicationAwareSubject.isAllowed(clusterStateAction.getAllowedScopes()));
    }
}
