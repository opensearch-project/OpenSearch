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
import org.opensearch.cluster.ApplicationManager;
import org.opensearch.extensions.ExtensionsManager;
import org.opensearch.extensions.ExtensionsSettings;
import org.opensearch.identity.ApplicationAwareSubject;
import org.opensearch.test.OpenSearchTestCase;
import static org.mockito.Mockito.spy;

public class ActionScopeTests extends OpenSearchTestCase {

    private ApplicationManager applicationManager;
    private ExtensionsManager extensionsManager;

    ExtensionsSettings.Extension expectedExtensionNode = new ExtensionsSettings.Extension(
        "firstExtension",
        "uniqueid1",
        "127.0.0.1",
        "9300",
        "0.0.7",
        "3.0.0",
        "3.0.0",
        Collections.emptyList(),
        null,
        List.of(ActionScope.READ)
    );

    @Before
    public void setup() throws IOException {

        extensionsManager = new ExtensionsManager(Set.of());
        applicationManager = spy(new ApplicationManager());
        extensionsManager.loadExtension(expectedExtensionNode);
        applicationManager.register(extensionsManager);
    }

    public void testAssignActionScopes() {

        Set<Scope> allowedScopes = Set.of(ActionScope.READ);

        ApplicationAwareSubject appSubject = spy(
            new ApplicationAwareSubject(applicationManager.getExtensionManager().getExtensionIdMap().get("uniqueid1"))
        );

        assertEquals(appSubject.getScopes(), allowedScopes);
    }

    public void testCallActionShouldFail() {

        Set<Scope> allowedScopes = Set.of(ActionScope.READ);
        ApplicationAwareSubject appSubject = spy(
            new ApplicationAwareSubject(applicationManager.getExtensionManager().getExtensionIdMap().get("uniqueid1"))
        );
        assertEquals(appSubject.getScopes(), allowedScopes);

        assertTrue(ApplicationManager.getInstance().isAllowed(appSubject, new ArrayList<>(allowedScopes)));

        ResizeAction resizeAction = ResizeAction.INSTANCE;
        ClusterStateAction clusterStateAction = ClusterStateAction.INSTANCE;

        assertFalse(ApplicationManager.getInstance().isAllowed(appSubject, resizeAction.getAllowedScopes()));
        assertFalse(ApplicationManager.getInstance().isAllowed(appSubject, clusterStateAction.getAllowedScopes()));
    }

    public void testCallActionShouldPass() {

        Set<Scope> allowedScopes = Set.of(ActionScope.READ);
        ApplicationAwareSubject appSubject = spy(
            new ApplicationAwareSubject(applicationManager.getExtensionManager().getExtensionIdMap().get("uniqueid1"))
        );
        assertEquals(appSubject.getScopes(), allowedScopes);

        assertTrue(ApplicationManager.getInstance().isAllowed(appSubject, new ArrayList<>(allowedScopes)));

        GetAction getAction = GetAction.INSTANCE;
        MultiGetAction multiGetAction = MultiGetAction.INSTANCE;
        assertTrue(ApplicationManager.getInstance().isAllowed(appSubject, getAction.getAllowedScopes()));
        assertTrue(ApplicationManager.getInstance().isAllowed(appSubject, multiGetAction.getAllowedScopes()));
    }
}
