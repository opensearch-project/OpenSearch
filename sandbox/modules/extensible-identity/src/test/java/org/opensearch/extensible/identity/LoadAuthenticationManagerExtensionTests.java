/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensible.identity;

import org.opensearch.authn.AuthenticationManager;
import org.opensearch.authn.Identity;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.List;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class LoadAuthenticationManagerExtensionTests extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ExtensibleIdentityPlugin.class, TestIdentityPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).put(nodeSettings()).build();
    }

    final Settings nodeSettings() {
        return Settings.builder()
            .put(ConfigConstants.IDENTITY_ENABLED, true)
            .put(
                ConfigConstants.IDENTITY_AUTH_MANAGER_CLASS,
                "org.opensearch.extensible.identity.TestIdentityPlugin.TestAuthenticationManager"
            )
            .build();
    }

    public void testLoadAuthManagerExtensionSuccess() throws Exception {
        ensureGreen();
        AuthenticationManager authManager = Identity.getAuthManager();
        assertNotNull(authManager);
        assertEquals(
            "org.opensearch.extensible.identity.TestIdentityPlugin.TestAuthenticationManager",
            authManager.getClass().getCanonicalName()
        );
    }
}
