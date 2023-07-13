/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.scopes;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.junit.Before;
import org.opensearch.OpenSearchException;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionResponse;
import org.opensearch.action.get.GetAction;
import org.opensearch.action.get.TransportGetAction;
import org.opensearch.common.settings.Settings;
import org.opensearch.extensions.ExtensionsManager;
import org.opensearch.extensions.ExtensionsSettings;
import org.opensearch.identity.ApplicationAwareSubject;
import org.opensearch.identity.IdentityService;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.wrappers.ScopeProtectedActionPlugin;
import org.opensearch.test.OpenSearchTestCase;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class ExtensionPointScopeTests extends OpenSearchTestCase {

    private ExtensionsManager extensionsManager;
    private IdentityService identityService;
    private ActionPlugin actionPlugin;
    private ApplicationAwareSubject appSubject;

    ExtensionsSettings.Extension expectedExtensionNode1 = new ExtensionsSettings.Extension(
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

    ExtensionsSettings.Extension expectedExtensionNode2 = new ExtensionsSettings.Extension(
        "secondExtension",
        "uniqueid2",
        "127.0.0.1",
        "9300",
        "0.0.7",
        "3.0.0",
        "3.0.0",
        Collections.emptyList(),
        null,
        List.of(ExtensionPointScope.ACTION)
    );

    @Before
    public void setup() throws IOException {

        extensionsManager = new ExtensionsManager(Set.of());
        extensionsManager.loadExtension(expectedExtensionNode1);
        extensionsManager.loadExtension(expectedExtensionNode2);
        identityService = spy(new IdentityService(extensionsManager, Settings.EMPTY, List.of()));
        actionPlugin = new ActionPlugin() {
            @Override
            public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
                return Arrays.asList(new ActionHandler<>(GetAction.INSTANCE, TransportGetAction.class));
            }
        };

    }

    public void testExtensionPointProtectedSubjectShouldPass() {
        appSubject = spy(new ApplicationAwareSubject(extensionsManager.getExtensionIdMap().get("uniqueid1"), extensionsManager));
        ScopeProtectedActionPlugin scopeProtectedActionPlugin = spy(new ScopeProtectedActionPlugin(actionPlugin, identityService));
        when(identityService.getSubject()).thenReturn(appSubject);
        Exception ex = assertThrows(OpenSearchException.class, scopeProtectedActionPlugin::getActions);
        assertTrue(ex.getMessage().contains("Unable to identify extension point scope: "));
    }

    public void testExtensionPointProtectedSubjectShouldFail() {
        appSubject = spy(new ApplicationAwareSubject(extensionsManager.getExtensionIdMap().get("uniqueid2"), extensionsManager));
        ScopeProtectedActionPlugin scopeProtectedActionPlugin = spy(new ScopeProtectedActionPlugin(actionPlugin, identityService));
        when(identityService.getSubject()).thenReturn(appSubject);
        assertEquals(scopeProtectedActionPlugin.getActions(), actionPlugin.getActions());
    }
}
