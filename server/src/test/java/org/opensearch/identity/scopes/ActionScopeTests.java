/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.scopes;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Before;
import org.opensearch.Version;
import org.opensearch.action.admin.cluster.state.ClusterStateAction;
import org.opensearch.action.admin.indices.shrink.ResizeAction;
import org.opensearch.action.get.GetAction;
import org.opensearch.action.get.MultiGetAction;
import org.opensearch.cluster.ApplicationManager;
import org.opensearch.common.transport.TransportAddress;
import org.opensearch.extensions.DiscoveryExtensionNode;
import org.opensearch.extensions.ExtensionsManager;
import org.opensearch.identity.ApplicationSubject;
import org.opensearch.identity.IdentityService;
import org.opensearch.identity.NamedPrincipal;
import org.opensearch.identity.Subject;
import org.opensearch.test.OpenSearchTestCase;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

public class ActionScopeTests extends OpenSearchTestCase {

    private Subject basicSubject;
    private ExtensionsManager extensionsManager;
    private ApplicationManager applicationManager;
    private IdentityService identityService;
    private Map<String, DiscoveryExtensionNode> extensionIdMap;

    private DiscoveryExtensionNode expectedExtensionNode = new DiscoveryExtensionNode(
        "firstExtension",
        "uniqueid1",
        new TransportAddress(InetAddress.getByName("127.0.0.0"), 9300),
        new HashMap<>(),
        Version.CURRENT,
        Version.CURRENT,
        List.of(),
        List.of(ActionScope.READ)
    );

    public ActionScopeTests() throws UnknownHostException {}

    @Before
    public void setup() {
        basicSubject = mock(Subject.class);
        identityService = mock(IdentityService.class);
        extensionsManager = mock(ExtensionsManager.class);
        applicationManager = spy(new ApplicationManager(extensionsManager));
        extensionIdMap = new HashMap<>();
    }

    public void testAssignActionScopes() {
        Set<Scope> allowedScopes = Set.of(ActionScope.READ);
        ScopedSubject subject = new ScopedSubject(allowedScopes);
        assertEquals(subject.getScopes(), allowedScopes);

        doReturn(extensionIdMap.keySet().stream().map(key -> (Principal) () -> key).collect(Collectors.toList())).when(extensionsManager)
            .getExtensionPrincipals();

        extensionIdMap.put(String.valueOf(subject.principal), expectedExtensionNode);
        ApplicationSubject appSubject = spy(new ApplicationSubject(subject));
        doReturn(subject.getScopes()).when(appSubject).getScopes();
        assertEquals(appSubject.getScopes(), allowedScopes);
    }

    public void testCallActionShouldFail() {
        Set<Scope> allowedScopes = Set.of(ActionScope.READ);
        ScopedSubject subject = new ScopedSubject(allowedScopes);
        assertEquals(subject.getScopes(), allowedScopes);

        doReturn(extensionIdMap.keySet().stream().map(key -> (Principal) () -> key).collect(Collectors.toList())).when(extensionsManager)
            .getExtensionPrincipals();

        extensionIdMap.put(String.valueOf(subject.principal), expectedExtensionNode);
        ApplicationSubject appSubject = spy(new ApplicationSubject(subject));

        doReturn(Optional.of(new NamedPrincipal("TestApplication"))).when(appSubject).getApplication();

        doReturn(allowedScopes).when(appSubject).getScopes();

        doReturn(true).when(appSubject).applicationExists();
        doReturn(allowedScopes.stream().map(Scope::asPermissionString).collect(Collectors.toSet())).when(appSubject).getScopes();

        assertTrue(ApplicationManager.getInstance().isAllowed(appSubject, new ArrayList<>(allowedScopes)));

        ResizeAction resizeAction = ResizeAction.INSTANCE;
        ClusterStateAction clusterStateAction = ClusterStateAction.INSTANCE;

        assertFalse(ApplicationManager.getInstance().isAllowed(appSubject, resizeAction.getAllowedScopes()));
        assertFalse(ApplicationManager.getInstance().isAllowed(appSubject, clusterStateAction.getAllowedScopes()));
    }

    public void testCallActionShouldPass() {

        Set<Scope> allowedScopes = Set.of(ActionScope.READ);
        ScopedSubject subject = new ScopedSubject(allowedScopes);
        assertEquals(subject.getScopes(), allowedScopes);

        doReturn(extensionIdMap.keySet().stream().map(key -> (Principal) () -> key).collect(Collectors.toList())).when(extensionsManager)
            .getExtensionPrincipals();

        extensionIdMap.put(String.valueOf(subject.principal), expectedExtensionNode);
        ApplicationSubject appSubject = spy(new ApplicationSubject(subject));

        doReturn(Optional.of(new NamedPrincipal("TestApplication"))).when(appSubject).getApplication();

        doReturn(allowedScopes).when(appSubject).getScopes();

        doReturn(true).when(appSubject).applicationExists();
        doReturn(allowedScopes.stream().map(Scope::asPermissionString).collect(Collectors.toSet())).when(appSubject).getScopes();

        assertTrue(ApplicationManager.getInstance().isAllowed(appSubject, new ArrayList<>(allowedScopes)));

        GetAction getAction = GetAction.INSTANCE;
        MultiGetAction multiGetAction = MultiGetAction.INSTANCE;
        assertTrue(ApplicationManager.getInstance().isAllowed(appSubject, getAction.getAllowedScopes()));
        assertTrue(ApplicationManager.getInstance().isAllowed(appSubject, multiGetAction.getAllowedScopes()));
    }
}
