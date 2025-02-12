/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.crypto;

import org.opensearch.cluster.metadata.CryptoMetadata;
import org.opensearch.common.crypto.CryptoHandler;
import org.opensearch.common.crypto.MasterKeyProvider;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.CryptoKeyProviderPlugin;
import org.opensearch.plugins.CryptoPlugin;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

public class CryptoHandlerRegistryTests extends OpenSearchTestCase {

    private TestCryptoHandlerRegistry cryptoManagerRegistry;
    private String pluginTypeWithCreationFailure;
    private CryptoKeyProviderPlugin cryptoPlugin1;
    private CryptoKeyProviderPlugin cryptoPlugin2;

    @Before
    public void setup() {
        List<CryptoKeyProviderPlugin> cryptoKPPlugins = new ArrayList<>();
        CryptoKeyProviderPlugin cryptoPlugin1 = Mockito.mock(CryptoKeyProviderPlugin.class);
        String pluginType1 = UUID.randomUUID().toString();
        Mockito.when(cryptoPlugin1.type()).thenReturn(pluginType1);
        MasterKeyProvider masterKeyProvider1 = Mockito.mock(MasterKeyProvider.class);
        Mockito.when(cryptoPlugin1.createKeyProvider(ArgumentMatchers.any())).thenReturn(masterKeyProvider1);
        this.cryptoPlugin1 = cryptoPlugin1;
        cryptoKPPlugins.add(cryptoPlugin1);

        CryptoKeyProviderPlugin cryptoPlugin2 = Mockito.mock(CryptoKeyProviderPlugin.class);
        String pluginType2 = UUID.randomUUID().toString();
        Mockito.when(cryptoPlugin2.type()).thenReturn(pluginType2);
        MasterKeyProvider masterKeyProvider2 = Mockito.mock(MasterKeyProvider.class);
        Mockito.when(cryptoPlugin2.createKeyProvider(ArgumentMatchers.any())).thenReturn(masterKeyProvider2);
        cryptoKPPlugins.add(cryptoPlugin2);
        this.cryptoPlugin2 = cryptoPlugin2;

        CryptoKeyProviderPlugin cryptoPluginCreationFailure = Mockito.mock(CryptoKeyProviderPlugin.class);
        pluginTypeWithCreationFailure = UUID.randomUUID().toString();
        Mockito.when(cryptoPluginCreationFailure.type()).thenReturn(pluginTypeWithCreationFailure);
        Mockito.when(cryptoPluginCreationFailure.createKeyProvider(ArgumentMatchers.any()))
            .thenThrow(new RuntimeException("Injected failure"));
        cryptoKPPlugins.add(cryptoPluginCreationFailure);

        cryptoManagerRegistry = new TestCryptoHandlerRegistry(new TestCryptoPlugin(), cryptoKPPlugins, Settings.EMPTY);
    }

    static class TestCryptoPlugin implements CryptoPlugin {

        @Override
        public CryptoHandler getOrCreateCryptoHandler(
            MasterKeyProvider keyProvider,
            String keyProviderName,
            String keyProviderType,
            Runnable onClose
        ) {
            return Mockito.mock(CryptoHandler.class);
        }
    }

    static class TestCryptoHandlerRegistry extends CryptoHandlerRegistry {

        protected TestCryptoHandlerRegistry(CryptoPlugin cryptoPlugin, List<CryptoKeyProviderPlugin> cryptoPlugins, Settings settings) {
            super(List.of(cryptoPlugin), cryptoPlugins, settings);
        }

        @Override
        public Map<String, CryptoKeyProviderPlugin> loadCryptoFactories(List<CryptoKeyProviderPlugin> cryptoKPPlugins) {
            return super.loadCryptoFactories(cryptoKPPlugins);
        }
    }

    public void testInitRegistryWithDuplicateKPType() {
        List<CryptoKeyProviderPlugin> cryptoPlugins = new ArrayList<>();
        CryptoKeyProviderPlugin cryptoPlugin1 = Mockito.mock(CryptoKeyProviderPlugin.class);
        String pluginType = UUID.randomUUID().toString();
        Mockito.when(cryptoPlugin1.type()).thenReturn(pluginType);
        cryptoPlugins.add(cryptoPlugin1);
        CryptoKeyProviderPlugin cryptoPlugin2 = Mockito.mock(CryptoKeyProviderPlugin.class);
        Mockito.when(cryptoPlugin2.type()).thenReturn(pluginType);
        cryptoPlugins.add(cryptoPlugin2);
        expectThrows(IllegalArgumentException.class, () -> cryptoManagerRegistry.loadCryptoFactories(cryptoPlugins));
    }

    public void testRegistry() {
        List<CryptoKeyProviderPlugin> cryptoPlugins = new ArrayList<>();
        CryptoKeyProviderPlugin cryptoPlugin1 = Mockito.mock(CryptoKeyProviderPlugin.class);
        String pluginType1 = UUID.randomUUID().toString();
        Mockito.when(cryptoPlugin1.type()).thenReturn(pluginType1);
        MasterKeyProvider masterKeyProvider1 = Mockito.mock(MasterKeyProvider.class);
        Mockito.when(cryptoPlugin1.createKeyProvider(Mockito.any())).thenReturn(masterKeyProvider1);
        cryptoPlugins.add(cryptoPlugin1);

        CryptoKeyProviderPlugin cryptoPlugin2 = Mockito.mock(CryptoKeyProviderPlugin.class);
        String pluginType2 = UUID.randomUUID().toString();
        Mockito.when(cryptoPlugin2.type()).thenReturn(pluginType2);
        MasterKeyProvider masterKeyProvider2 = Mockito.mock(MasterKeyProvider.class);
        Mockito.when(cryptoPlugin2.createKeyProvider(Mockito.any())).thenReturn(masterKeyProvider2);
        cryptoPlugins.add(cryptoPlugin2);

        Map<String, CryptoKeyProviderPlugin> loadedPlugins = cryptoManagerRegistry.loadCryptoFactories(cryptoPlugins);

        CryptoKeyProviderPlugin keyProviderPlugin = loadedPlugins.get(pluginType1);
        assertNotNull(keyProviderPlugin);
        assertEquals(cryptoPlugin1, keyProviderPlugin);

        keyProviderPlugin = loadedPlugins.get(pluginType2);
        assertNotNull(keyProviderPlugin);
        assertEquals(cryptoPlugin2, keyProviderPlugin);
    }

    public void testCryptoManagerMissing() {
        String pluginName = UUID.randomUUID().toString();
        String pluginType = UUID.randomUUID().toString();
        CryptoMetadata cryptoMetadata = new CryptoMetadata(pluginName, pluginType, Settings.EMPTY);
        expectThrows(CryptoRegistryException.class, () -> cryptoManagerRegistry.fetchCryptoHandler(cryptoMetadata));
    }

    public void testCryptoManagerCreationFailure() {
        String pluginName = UUID.randomUUID().toString();
        CryptoMetadata cryptoMetadata = new CryptoMetadata(pluginName, pluginTypeWithCreationFailure, Settings.EMPTY);
        expectThrows(CryptoRegistryException.class, () -> cryptoManagerRegistry.fetchCryptoHandler(cryptoMetadata));
    }

    public void testCryptoManagerCreationSuccess() {

        String pluginName1 = UUID.randomUUID().toString();
        CryptoMetadata cryptoMetadata = new CryptoMetadata(pluginName1, cryptoPlugin1.type(), Settings.EMPTY);
        CryptoHandler cryptoHandler = cryptoManagerRegistry.fetchCryptoHandler(cryptoMetadata);
        assertNotNull(cryptoHandler);

        String pluginName2 = UUID.randomUUID().toString();
        CryptoHandler cryptoHandler2 = cryptoManagerRegistry.fetchCryptoHandler(
            new CryptoMetadata(pluginName2, cryptoPlugin2.type(), Settings.EMPTY)
        );
        assertNotNull(cryptoHandler2);
        CryptoHandler cryptoHandler3 = cryptoManagerRegistry.fetchCryptoHandler(
            new CryptoMetadata(pluginName1, cryptoPlugin1.type(), Settings.EMPTY)
        );
        assertNotNull(cryptoHandler3);
        assertEquals(cryptoHandler, cryptoHandler3);
        assertNotEquals(cryptoHandler2, cryptoHandler);

        CryptoHandler cryptoHandlerNewType = cryptoManagerRegistry.fetchCryptoHandler(
            new CryptoMetadata(pluginName1, cryptoPlugin2.type(), Settings.EMPTY)
        );
        assertNotNull(cryptoHandlerNewType);
        assertNotEquals(cryptoHandler, cryptoHandlerNewType);
        assertNotEquals(cryptoHandler2, cryptoHandlerNewType);
        assertNotEquals(cryptoHandler3, cryptoHandlerNewType);
    }
}
