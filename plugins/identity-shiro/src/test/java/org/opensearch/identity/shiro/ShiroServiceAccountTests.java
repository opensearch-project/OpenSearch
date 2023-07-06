/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.shiro;

import java.util.Arrays;
import java.util.List;
import org.junit.Before;
import org.opensearch.cluster.ApplicationManager;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.env.TestEnvironment;
import org.opensearch.identity.IdentityService;
import org.opensearch.identity.ServiceAccount;
import org.opensearch.index.IndexModule;
import org.opensearch.plugins.IdentityPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginsService;
import org.opensearch.test.OpenSearchTestCase;

public class ShiroServiceAccountTests extends OpenSearchTestCase {


    private ShiroServiceAccountManager shiroServiceAccountManager;
    private ApplicationManager applicationManager;
    private PluginsService pluginsService;
    private IdentityService identityService;
    private IdentityPlugin identityPlugin;
    private AdditionalSettingsPlugin1 additionalSettingsPlugin1;
    private AdditionalSettingsPlugin2 additionalSettingsPlugin2;

     Settings settings = Settings.builder()
        .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
        .put("my.setting", "test")
            .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), IndexModule.Type.NIOFS.getSettingsKey())
        .build();

    static PluginsService newPluginsService(Settings settings, ApplicationManager applicationManager,
        Class<? extends Plugin>... classpathPlugins) {
        return new PluginsService(
            settings,
            applicationManager,
            null,
            null,
            TestEnvironment.newEnvironment(settings).pluginsDir(),
            Arrays.asList(classpathPlugins)
        );
    }

    public static class AdditionalSettingsPlugin1 extends Plugin {
        @Override
        public Settings additionalSettings() {
            return Settings.builder()
                .put("foo.bar", "1")
                .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), IndexModule.Type.MMAPFS.getSettingsKey())
                .build();
        }
    }

    public static class AdditionalSettingsPlugin2 extends Plugin {
        @Override
        public Settings additionalSettings() {
            return Settings.builder().put("foo.bar", "2").build();
        }
    }

    @Before
    public void setup() {
        identityPlugin = new ShiroIdentityPlugin(Settings.EMPTY);
        List<IdentityPlugin> pluginList = List.of(identityPlugin);
        applicationManager = new ApplicationManager();
        identityService = new IdentityService(Settings.EMPTY, pluginList);
        additionalSettingsPlugin1 = new AdditionalSettingsPlugin1();
        additionalSettingsPlugin2 = new AdditionalSettingsPlugin2();
        shiroServiceAccountManager = new ShiroServiceAccountManager();
    }


    public void testRegisterSinglePlugin() {
        pluginsService = newPluginsService(settings, applicationManager, additionalSettingsPlugin1.getClass());
        ServiceAccount serviceAccount = shiroServiceAccountManager.getServiceAccount(pluginsService.plugins.get(0).v1());
        assertEquals(pluginsService.plugins.get(0).v1().getPrincipal(), serviceAccount.getName());
    }

    public void testRegisterMultiplePlugins() {

    }

    public void testRegisterMultiplePluginsWithSameName() {

    }

    public void testGetServiceAccountOfRegisteredPlugin() {

    }

    public void testGetServiceAccountOfUnregisteredPlugin() {

    }
}
