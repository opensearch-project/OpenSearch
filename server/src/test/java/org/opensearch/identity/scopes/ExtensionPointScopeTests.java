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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Before;
import org.opensearch.Version;
import org.opensearch.cluster.ApplicationManager;
import org.opensearch.common.transport.TransportAddress;
import org.opensearch.extensions.DiscoveryExtensionNode;
import org.opensearch.extensions.ExtensionsManager;
import org.opensearch.identity.IdentityService;
import org.opensearch.identity.NamedPrincipal;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.wrappers.ScopeProtectedActionPlugin;
import org.opensearch.test.OpenSearchTestCase;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * This test class tests the functionality of teh ExtensionPointScope scopes.
 * This class currently makes heavy use of mocks which limited its functionality to the direct unit testing of
 */
public class ExtensionPointScopeTests extends OpenSearchTestCase {

    ApplicationManager applicationManager;
    ExtensionsManager extensionsManager;
    IdentityService identityService;
    Map<String, DiscoveryExtensionNode> extensionMap;
    ScopeProtectedActionPlugin wrappedPlugin;
    ActionPlugin mockedActionPlugin;

    NamedPrincipal namedPrincipal1 = new NamedPrincipal("uniqueid1");

    DiscoveryExtensionNode extensionNode1 = new DiscoveryExtensionNode(
        "firstExtension",
        "uniqueid1",
        new TransportAddress(InetAddress.getByName("127.0.0.0"), 9300),
        new HashMap<String, String>(),
        Version.fromString("3.0.0"),
        Version.fromString("3.0.0"),
        Collections.emptyList(),
        List.of(Scope.parseScopeFromString("EXTENSION_POINT.ACTION_PLUGIN.IMPLEMENT"))
    );

    DiscoveryExtensionNode extensionNode2 = new DiscoveryExtensionNode(
        "secondExtension",
        "uniqueid2",
        new TransportAddress(InetAddress.getByName("127.0.0.0"), 9300),
        new HashMap<String, String>(),
        Version.fromString("3.0.0"),
        Version.fromString("3.0.0"),
        Collections.emptyList(),
        List.of()
    );

    public ExtensionPointScopeTests() throws UnknownHostException {}

    @Before
    public void setup() throws UnknownHostException {
        identityService = mock(IdentityService.class);
        extensionsManager = mock(ExtensionsManager.class);
        applicationManager = spy(new ApplicationManager(extensionsManager));

        mockedActionPlugin = mock(ActionPlugin.class);
        wrappedPlugin = spy(new ScopeProtectedActionPlugin(mockedActionPlugin, identityService));

        when(extensionsManager.getExtensionIdMap()).thenReturn(extensionMap);

        extensionMap = new HashMap<>();
        extensionMap.put("uniqueid1", extensionNode1);
        extensionMap.put("uniqueid2", extensionNode2);
    }

    public void testActionPluginShouldPass() {

        // Redirect scope request from whatever the subject is to an extension registered with the ExtensionManager
        // Todo: This will need to be swapped once Plugins acting as Extensions is implemented.
        doReturn(namedPrincipal1).when(wrappedPlugin).getPrincipal();
        doReturn(extensionMap.get("uniqueid1").getScopes()).when(applicationManager).getScopes(namedPrincipal1);
        assertEquals(applicationManager.getScopes(wrappedPlugin.getPrincipal()), Set.of(ExtensionPointScope.ACTION.asPermissionString()));

        // Extract the functionality of the wrapped plugin method getTaskHeaders and check is allowed
        // Cannot call methods in class directly since require extensionManager, applicationManager; will require further development before
        // meaningful tests
    }
}
