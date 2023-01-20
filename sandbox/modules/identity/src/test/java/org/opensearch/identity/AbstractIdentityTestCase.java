/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.extensible.identity.ExtensibleIdentityPlugin;
import org.opensearch.http.CorsHandler;
import org.opensearch.http.HttpTransportSettings;
import org.opensearch.identity.authmanager.internal.InternalAuthenticationManager;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.Netty4ModulePlugin;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Base test case for integration tests against the identity plugin.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public abstract class AbstractIdentityTestCase extends OpenSearchIntegTestCase {
    public static class TestRegisterExtendedPluginsSettingPlugin extends Plugin {

        @Override
        public List<Setting<?>> getSettings() {
            return Collections.singletonList(Setting.simpleString("extended.plugins", Setting.Property.NodeScope));
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(
            TestRegisterExtendedPluginsSettingPlugin.class,
            ExtensibleIdentityPlugin.class,
            IdentityPlugin.class,
            Netty4ModulePlugin.class
        );
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).put(nodeSettings()).build();
    }

    final Settings nodeSettings() {
        return Settings.builder()
            .put(HttpTransportSettings.SETTING_CORS_ENABLED.getKey(), true)
            .put(HttpTransportSettings.SETTING_CORS_ALLOW_ORIGIN.getKey(), CorsHandler.ANY_ORIGIN)
            .put(HttpTransportSettings.SETTING_CORS_ALLOW_CREDENTIALS.getKey(), true)
            .put(ConfigConstants.IDENTITY_ENABLED, true)
            .put(ConfigConstants.IDENTITY_AUTH_MANAGER_CLASS, InternalAuthenticationManager.class.getCanonicalName())
            .put("extended.plugins", ExtensibleIdentityPlugin.class.getCanonicalName())
            .build();
    }
}
