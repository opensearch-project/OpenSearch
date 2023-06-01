/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.bootstrap;

import java.util.List;
import org.opensearch.OpenSearchException;
import org.opensearch.common.settings.Settings;
import org.opensearch.identity.IdentityService;
import org.opensearch.identity.noop.NoopIdentityPlugin;
import org.opensearch.identity.noop.NoopScheduledJobIdentityManager;
import org.opensearch.plugins.IdentityPlugin;
import org.opensearch.test.OpenSearchTestCase;

public class IdentityPluginTests extends OpenSearchTestCase {

    public void testSingleIdentityPluginSucceeds() {
        IdentityPlugin identityPlugin1 = new NoopIdentityPlugin();
        List<IdentityPlugin> pluginList = List.of(identityPlugin1);
        IdentityService identityService = new IdentityService(Settings.EMPTY, pluginList);
        assertTrue(identityService.getSubject().getPrincipal().getName().equalsIgnoreCase("Unauthenticated"));
    }

    public void testMultipleIdentityPluginsFail() {
        IdentityPlugin identityPlugin1 = new NoopIdentityPlugin();
        IdentityPlugin identityPlugin2 = new NoopIdentityPlugin();
        IdentityPlugin identityPlugin3 = new NoopIdentityPlugin();
        List<IdentityPlugin> pluginList = List.of(identityPlugin1, identityPlugin2, identityPlugin3);
        Exception ex = assertThrows(OpenSearchException.class, () -> new IdentityService(Settings.EMPTY, pluginList));
        assert (ex.getMessage().contains("Multiple identity plugins are not supported,"));
    }

    public void testIdentityServiceGetScheduledJobIdentityManager() {
        IdentityPlugin identityPlugin1 = new NoopIdentityPlugin();
        List<IdentityPlugin> pluginList = List.of(identityPlugin1);
        IdentityService identityService = new IdentityService(Settings.EMPTY, pluginList);
        assertNotNull(identityService.getScheduledJobIdentityManager());
        assertTrue(identityService.getScheduledJobIdentityManager() instanceof NoopScheduledJobIdentityManager);
    }
}
