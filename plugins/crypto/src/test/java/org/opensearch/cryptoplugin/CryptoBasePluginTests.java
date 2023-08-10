/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.cryptoplugin;

import org.junit.Assert;
import org.opensearch.common.settings.Settings;
import org.opensearch.cryptospi.CryptoKeyProviderExtension;
import org.opensearch.plugins.ExtensiblePlugin;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.crypto.CryptoManager;

import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CryptoBasePluginTests extends OpenSearchTestCase {

    public void testExtensionNotAvailableFailure() {
        CryptoBasePlugin cryptoBasePlugin = new CryptoBasePlugin();
        CryptoManager.Factory factory = cryptoBasePlugin.createClientFactory("unavailable-extension");
        Assert.assertThrows(IllegalArgumentException.class, () -> factory.create(Settings.EMPTY, "unavailable-name"));
    }

    public void testLoadUnavailableExtension() {
        CryptoBasePlugin cryptoBasePlugin = new CryptoBasePlugin();
        ExtensiblePlugin.ExtensionLoader mockLoader = mock(ExtensiblePlugin.ExtensionLoader.class);
        when(mockLoader.loadExtensions(any())).thenReturn(Collections.emptyList());
        Assert.assertThrows(IllegalArgumentException.class, () -> cryptoBasePlugin.loadExtensions(mockLoader));
    }

    public void testSameInstanceForKeyProviderNameAndType() {
        CryptoKeyProviderExtension keyProviderExtension = new MockExtensionPlugin();
        CryptoBasePlugin cryptoBasePlugin = new CryptoBasePlugin();
        String keyProviderName = "test-1";
        CryptoManager cryptoClient1 = createTestCryptoManager(keyProviderExtension, cryptoBasePlugin, keyProviderName);
        CryptoManager cryptoClient2 = createTestCryptoManager(keyProviderExtension, cryptoBasePlugin, keyProviderName);
        Assert.assertEquals(cryptoClient1, cryptoClient2);

        String keyProviderName2 = "test-2";
        CryptoManager cryptoClient3 = createTestCryptoManager(keyProviderExtension, cryptoBasePlugin, keyProviderName2);
        Assert.assertNotEquals(cryptoClient3, cryptoClient2);

        CryptoManagerImpl cryptoManagerImpl = cryptoBasePlugin.keyProviderCryptoManagers.get(keyProviderExtension.type())
            .get(keyProviderName);
        cryptoManagerImpl.closeInternal();
        Assert.assertEquals(1, cryptoBasePlugin.keyProviderCryptoManagers.get(keyProviderExtension.type()).size());
        CryptoManager cryptoClient4 = createTestCryptoManager(keyProviderExtension, cryptoBasePlugin, keyProviderName);
        Assert.assertNotEquals(cryptoClient4, cryptoClient2);

        cryptoManagerImpl = cryptoBasePlugin.keyProviderCryptoManagers.get(keyProviderExtension.type()).get(keyProviderName);
        cryptoManagerImpl.closeInternal();
        Assert.assertEquals(1, cryptoBasePlugin.keyProviderCryptoManagers.get(keyProviderExtension.type()).size());
        cryptoManagerImpl = cryptoBasePlugin.keyProviderCryptoManagers.get(keyProviderExtension.type()).get(keyProviderName2);
        cryptoManagerImpl.closeInternal();
        Assert.assertEquals(0, cryptoBasePlugin.keyProviderCryptoManagers.size());
    }

    public static CryptoManager createTestCryptoManager(
        CryptoKeyProviderExtension keyProviderExtension,
        CryptoBasePlugin cryptoBasePlugin,
        String keyProviderName
    ) {
        ExtensiblePlugin.ExtensionLoader mockLoader = mock(ExtensiblePlugin.ExtensionLoader.class);
        when(mockLoader.loadExtensions(any())).thenReturn(List.of(keyProviderExtension));
        cryptoBasePlugin.loadExtensions(mockLoader);

        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.scheduler()).thenReturn(new Scheduler.SafeScheduledThreadPoolExecutor(4));
        cryptoBasePlugin.createComponents(null, null, threadPool, null, null, null, null, null, null, null, null);
        CryptoManager.Factory factory = cryptoBasePlugin.createClientFactory(keyProviderExtension.type());
        return factory.create(Settings.EMPTY, keyProviderName);
    }
}
