/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.bootstrap;

import org.opensearch.OpenSearchException;
import org.opensearch.client.Client;
import org.opensearch.common.settings.Settings;
import org.opensearch.identity.IdentityService;
import org.opensearch.identity.noop.NoopIdentityPlugin;
import org.opensearch.identity.noop.NoopTokenManager;
import org.opensearch.plugins.IdentityPlugin;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;

import java.util.List;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IdentityPluginTests extends OpenSearchTestCase {

    public void testSingleIdentityPluginSucceeds() {
        Client client = mock(Client.class);
        TestThreadPool threadPool = new TestThreadPool(getTestName());
        when(client.threadPool()).thenReturn(threadPool);
        IdentityPlugin identityPlugin1 = new NoopIdentityPlugin(client);
        List<IdentityPlugin> pluginList1 = List.of(identityPlugin1);
        IdentityService identityService1 = new IdentityService(Settings.EMPTY, client, pluginList1);
        assertTrue(identityService1.getCurrentSubject().getPrincipal().getName().equalsIgnoreCase("Unauthenticated"));
        assertThat(identityService1.getTokenManager(), is(instanceOf(NoopTokenManager.class)));
        terminate(threadPool);
    }

    public void testMultipleIdentityPluginsFail() {
        Client client = mock(Client.class);
        TestThreadPool threadPool = new TestThreadPool(getTestName());
        when(client.threadPool()).thenReturn(threadPool);
        IdentityPlugin identityPlugin1 = new NoopIdentityPlugin(client);
        IdentityPlugin identityPlugin2 = new NoopIdentityPlugin(client);
        IdentityPlugin identityPlugin3 = new NoopIdentityPlugin(client);
        List<IdentityPlugin> pluginList = List.of(identityPlugin1, identityPlugin2, identityPlugin3);
        Exception ex = assertThrows(OpenSearchException.class, () -> new IdentityService(Settings.EMPTY, client, pluginList));
        assert (ex.getMessage().contains("Multiple identity plugins are not supported,"));
        terminate(threadPool);
    }
}
