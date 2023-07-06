/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.crypto;

import org.junit.Assert;
import org.opensearch.common.settings.Settings;
import org.opensearch.cryptospi.CryptoKeyProviderExtension;
import org.opensearch.plugins.ExtensiblePlugin;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CryptoBasePluginTests {

    public void testExtensionNotAvailableFailure() {
        CryptoBasePlugin cryptoBasePlugin = new CryptoBasePlugin();
        CryptoClient.Factory factory = cryptoBasePlugin.createClientFactory("unavailable-extension");
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
        CryptoClient cryptoClient1 = createTestCryptoClient(keyProviderExtension, cryptoBasePlugin, keyProviderName);
        CryptoClient cryptoClient2 = createTestCryptoClient(keyProviderExtension, cryptoBasePlugin, keyProviderName);
        Assert.assertEquals(cryptoClient1, cryptoClient2);

        String keyProviderName2 = "test-2";
        CryptoClient cryptoClient3 = createTestCryptoClient(keyProviderExtension, cryptoBasePlugin, keyProviderName2);
        Assert.assertNotEquals(cryptoClient3, cryptoClient2);

        CryptoStore cryptoStore = cryptoBasePlugin.keyProviderCryptoClients.get(keyProviderExtension.type()).get(keyProviderName);
        cryptoStore.closeInternal();
        Assert.assertEquals(1, cryptoBasePlugin.keyProviderCryptoClients.get(keyProviderExtension.type()).size());
        CryptoClient cryptoClient4 = createTestCryptoClient(keyProviderExtension, cryptoBasePlugin, keyProviderName);
        Assert.assertNotEquals(cryptoClient4, cryptoClient2);

        cryptoStore = cryptoBasePlugin.keyProviderCryptoClients.get(keyProviderExtension.type()).get(keyProviderName);
        cryptoStore.closeInternal();
        Assert.assertEquals(1, cryptoBasePlugin.keyProviderCryptoClients.get(keyProviderExtension.type()).size());
        cryptoStore = cryptoBasePlugin.keyProviderCryptoClients.get(keyProviderExtension.type()).get(keyProviderName2);
        cryptoStore.closeInternal();
        Assert.assertEquals(0, cryptoBasePlugin.keyProviderCryptoClients.size());
    }

    public static CryptoClient createTestCryptoClient(
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
        CryptoClient.Factory factory = cryptoBasePlugin.createClientFactory(keyProviderExtension.type());
        return factory.create(Settings.EMPTY, keyProviderName);
    }
}
