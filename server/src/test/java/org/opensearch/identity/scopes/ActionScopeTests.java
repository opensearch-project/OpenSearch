/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.scopes;

import java.util.ArrayList;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Before;
import org.opensearch.OpenSearchException;
import org.opensearch.action.admin.cluster.state.ClusterStateAction;
import org.opensearch.action.admin.indices.shrink.ResizeAction;
import org.opensearch.action.get.GetAction;
import org.opensearch.action.get.MultiGetAction;
import org.opensearch.cluster.ApplicationManager;
import org.opensearch.extensions.ExtensionsManager;
import org.opensearch.identity.ApplicationSubject;
import org.opensearch.identity.IdentityService;
import org.opensearch.identity.ScopeAwareSubject;
import org.opensearch.identity.Subject;
import org.opensearch.test.OpenSearchTestCase;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class ActionScopeTests extends OpenSearchTestCase {

    Subject basicSubject;
    ScopeAwareSubject scopeAwareSubject;
    ApplicationManager applicationManager;
    ExtensionsManager extensionsManager;
    IdentityService identityService;

    @Before
    public void setup() {
        basicSubject = mock(Subject.class);
        scopeAwareSubject = mock(ScopeAwareSubject.class);
        identityService = mock(IdentityService.class);
        extensionsManager = mock(ExtensionsManager.class);
        applicationManager = new ApplicationManager(extensionsManager);
    }

    public void testAssignActionScopes() {

        ScopedSubject subject = new ScopedSubject();
        Set<Scope> allowedScopes = Set.of(ActionScope.READ);
        subject.setScopes(allowedScopes);
        assertEquals(subject.getScopes(), allowedScopes.stream().map((scope) -> toString()).collect(Collectors.toSet()));

        ApplicationSubject appSubject = new ApplicationSubject(subject);
        assertEquals(appSubject.getScopes(), allowedScopes.stream().map((scope) -> toString()).collect(Collectors.toSet()));

        // Should fail to assign scopes to ApplicationSubject directly
        appSubject = new ApplicationSubject(basicSubject);
        allowedScopes = Set.of(ActionScope.READ);
        ApplicationSubject finalAppSubject = appSubject;
        Set<Scope> finalAllowedScopes = allowedScopes;
        final var exception = Assert.assertThrows(OpenSearchException.class, () -> finalAppSubject.setScopes(finalAllowedScopes));
        assertTrue(exception.getMessage().contains("Could not set scopes of ApplicationSubject"));
    }

    public void testCallActionShouldFail() {

        ScopedSubject subject = new ScopedSubject();
        Set<Scope> allowedScopes = Set.of(ActionScope.READ);
        subject.setScopes(allowedScopes);
        assertEquals(subject.getScopes(), allowedScopes.stream().map((scope) -> toString()).collect(Collectors.toSet()));
        ApplicationSubject appSubject = new ApplicationSubject(subject);
        assertEquals(appSubject.getScopes(), allowedScopes.stream().map((scope) -> toString()).collect(Collectors.toSet()));

        doReturn(true).when(appSubject).applicationExists();
        doReturn(allowedScopes.stream().map(Scope::asPermissionString).collect(Collectors.toSet())).when(appSubject).applicationExists();

        assertTrue(ApplicationManager.getInstance().isAllowed(appSubject, new ArrayList<>(allowedScopes)));

        ResizeAction resizeAction = ResizeAction.INSTANCE;
        ClusterStateAction clusterStateAction = ClusterStateAction.INSTANCE;
        assertFalse(ApplicationManager.getInstance().isAllowed(appSubject, resizeAction.getAllowedScopes()));
        assertFalse(ApplicationManager.getInstance().isAllowed(appSubject, clusterStateAction.getAllowedScopes()));
    }

    public void testCallActionShouldPass() {

        ScopedSubject subject = new ScopedSubject();
        Set<Scope> allowedScopes = Set.of(ActionScope.READ);
        subject.setScopes(allowedScopes);
        assertEquals(subject.getScopes(), allowedScopes.stream().map((scope) -> toString()).collect(Collectors.toSet()));
        ApplicationSubject appSubject = new ApplicationSubject(subject);
        assertEquals(appSubject.getScopes(), allowedScopes.stream().map((scope) -> toString()).collect(Collectors.toSet()));

        doReturn(true).when(appSubject).applicationExists();
        doReturn(allowedScopes.stream().map(Scope::asPermissionString).collect(Collectors.toSet())).when(appSubject).applicationExists();

        assertTrue(ApplicationManager.getInstance().isAllowed(appSubject, new ArrayList<>(allowedScopes)));

        GetAction getAction = GetAction.INSTANCE;
        MultiGetAction multiGetAction = MultiGetAction.INSTANCE;
        assertTrue(ApplicationManager.getInstance().isAllowed(appSubject, getAction.getAllowedScopes()));
        assertTrue(ApplicationManager.getInstance().isAllowed(appSubject, multiGetAction.getAllowedScopes()));
    }
}
