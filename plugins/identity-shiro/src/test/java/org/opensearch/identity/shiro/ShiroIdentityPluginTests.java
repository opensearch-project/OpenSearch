/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.shiro;

import java.io.IOException;
import java.util.List;
import org.opensearch.OpenSearchException;
import org.opensearch.cluster.ApplicationManager;
import org.opensearch.common.settings.Settings;
import org.opensearch.extensions.NoopExtensionsManager;
import org.opensearch.identity.IdentityService;
import org.opensearch.identity.noop.NoopServiceAccountManager;
import org.opensearch.plugins.IdentityPlugin;
import org.opensearch.test.OpenSearchTestCase;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class ShiroIdentityPluginTests extends OpenSearchTestCase {

    public void testSingleIdentityPluginSucceeds() throws IOException {
        IdentityPlugin identityPlugin1 = new ShiroIdentityPlugin(
            Settings.EMPTY,
            new ApplicationManager(new NoopExtensionsManager(), null, new NoopServiceAccountManager())
        );
        List<IdentityPlugin> pluginList1 = List.of(identityPlugin1);
        IdentityService identityService1 = new IdentityService(Settings.EMPTY, pluginList1);
        assertThat(identityService1.getTokenManager(), is(instanceOf(ShiroTokenManager.class)));
    }

    public void testMultipleIdentityPluginsFail() throws IOException {
        IdentityPlugin identityPlugin1 = new ShiroIdentityPlugin(
            Settings.EMPTY,
            new ApplicationManager(new NoopExtensionsManager(), null, new NoopServiceAccountManager())
        );
        IdentityPlugin identityPlugin2 = new ShiroIdentityPlugin(
            Settings.EMPTY,
            new ApplicationManager(new NoopExtensionsManager(), null, new NoopServiceAccountManager())
        );
        IdentityPlugin identityPlugin3 = new ShiroIdentityPlugin(
            Settings.EMPTY,
            new ApplicationManager(new NoopExtensionsManager(), null, new NoopServiceAccountManager())
        );
        List<IdentityPlugin> pluginList = List.of(identityPlugin1, identityPlugin2, identityPlugin3);
        Exception ex = assertThrows(OpenSearchException.class, () -> new IdentityService(Settings.EMPTY, pluginList));
        assert (ex.getMessage().contains("Multiple identity plugins are not supported,"));
    }

}
