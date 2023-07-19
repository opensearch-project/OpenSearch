/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.scopes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.junit.Before;
import org.opensearch.action.admin.cluster.state.ClusterStateAction;
import org.opensearch.action.admin.indices.shrink.ResizeAction;
import org.opensearch.action.get.GetAction;
import org.opensearch.action.get.MultiGetAction;
import org.opensearch.extensions.ExtensionsManager;
import org.opensearch.extensions.ExtensionsSettings;
import org.opensearch.identity.ApplicationManager;
import org.opensearch.identity.ApplicationAwareSubject;
import org.opensearch.test.OpenSearchTestCase;
import static org.mockito.Mockito.spy;
import static org.hamcrest.MatcherAssert.assertThat;

public class ScopeTests extends OpenSearchTestCase {

    private final ApplicationManager applicationManager;

    @Before
    public void setup() throws IOException {
    }

    //TODO: Review coverage

    public void testScopes() {
        assertEquals(ActionScope.ALL.asPermissionString(), "ACTION.CLUSTER.ALL");
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> Scope.parseScopeFromString("INVALID"));
        assertTrue(ex.getMessage().contains("Invalid scope format"));
        RuntimeException ex2 = assertThrows(RuntimeException.class, () -> Scope.parseScopeFromString("ACTION.CLUSTER.INVALID"));
        assertTrue(ex2.getMessage().contains("Failed to find scope"));
        RuntimeException ex3 = assertThrows(RuntimeException.class, () -> Scope.parseScopeFromString("APPLICATION.CLUSTER.INVALID"));
        assertTrue(ex3.getMessage().contains("Failed to find scope"));
        RuntimeException ex4 = assertThrows(RuntimeException.class, () -> Scope.parseScopeFromString("EXTENSION_POINT.CLUSTER.INVALID"));
        assertTrue(ex4.getMessage().contains("Failed to find scope"));
        RuntimeException ex5 = assertThrows(RuntimeException.class, () -> Scope.parseScopeFromString("NAMESPACE.CLUSTER.ACTION"));
        assertTrue(ex5.getMessage(), ex5.getMessage().contains("Unknown ScopeNamespace"));
        RuntimeException ex6 = assertThrows(RuntimeException.class, () -> Scope.parseScopeFromString("ACTION.INVALID.ACTION"));
        assertTrue(ex6.getMessage(), ex6.getMessage().contains("Unknown ScopeArea"));
        assertEquals(ScopeEnums.ScopeArea.fromString("APPLICATION"), ScopeEnums.ScopeArea.APPLICATION);
        assertEquals(ScopeEnums.ScopeNamespace.fromString("APPLICATION"), ScopeEnums.ScopeNamespace.APPLICATION);
    }

    public void testAssignActionScopes() {

        Set<Scope> allowedScopes = Set.of(ActionScope.READ);

        ApplicationAwareSubject appSubject = spy(
            new ApplicationAwareSubject(, applicationManager)
        );

        assertEquals(appSubject.getScopes(), allowedScopes);
    }

    public void testCallActionShouldFail() {

        Set<Scope> allowedScopes = Set.of(ActionScope.READ);
        ApplicationAwareSubject appSubject = spy(
            new ApplicationAwareSubject(extensionsManager.getExtensionIdMap().get("uniqueid1"), applicationManager)
        );
        assertEquals(appSubject.getScopes(), allowedScopes);

        assertTrue(appSubject.isAllowed(new ArrayList<>(allowedScopes)));

        ResizeAction resizeAction = ResizeAction.INSTANCE;
        ClusterStateAction clusterStateAction = ClusterStateAction.INSTANCE;

        assertFalse(appSubject.isAllowed(resizeAction.getAllowedScopes()));
        assertFalse(appSubject.isAllowed(clusterStateAction.getAllowedScopes()));
    }

    public void testCallActionShouldPass() {

        Set<Scope> allowedScopes = Set.of(ActionScope.READ);
        ApplicationAwareSubject appSubject = spy(
            new ApplicationAwareSubject(extensionsManager.getExtensionIdMap().get("uniqueid1"), applicationManager)
        );
        assertEquals(appSubject.getScopes(), allowedScopes);

        assertTrue(appSubject.isAllowed(new ArrayList<>(allowedScopes)));

        GetAction getAction = GetAction.INSTANCE;
        MultiGetAction multiGetAction = MultiGetAction.INSTANCE;
        assertTrue(appSubject.isAllowed(getAction.getAllowedScopes()));
        assertTrue(appSubject.isAllowed(multiGetAction.getAllowedScopes()));
    }
}
