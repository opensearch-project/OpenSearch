/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.crypto;

import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.opensearch.cluster.metadata.CryptoMetadata;
import org.opensearch.common.SetOnce;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.set.Sets;
import org.opensearch.plugins.CryptoPlugin;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class CryptoManagerRegistryTests extends OpenSearchTestCase {

    public void testInitRegistryWithDuplicateExtension() {
        CryptoManagerRegistry.registry = new SetOnce<>();
        List<CryptoPlugin> cryptoPlugins = new ArrayList<>();
        CryptoPlugin cryptoPlugin1 = Mockito.mock(CryptoPlugin.class);
        String pluginType = UUID.randomUUID().toString();
        Mockito.when(cryptoPlugin1.getKeyProviderTypes()).thenReturn(Sets.newHashSet(pluginType));
        cryptoPlugins.add(cryptoPlugin1);
        CryptoPlugin cryptoPlugin2 = Mockito.mock(CryptoPlugin.class);
        Mockito.when(cryptoPlugin2.getKeyProviderTypes()).thenReturn(Sets.newHashSet(pluginType));
        cryptoPlugins.add(cryptoPlugin2);
        CryptoManagerRegistryTests.expectThrows(IllegalArgumentException.class, () -> CryptoManagerRegistry.initRegistry(cryptoPlugins));
    }

    public void testRegistry() {
        CryptoManagerRegistry.registry = new SetOnce<>();
        List<CryptoPlugin> cryptoPlugins = new ArrayList<>();
        CryptoPlugin cryptoPlugin1 = Mockito.mock(CryptoPlugin.class);
        String pluginType1 = UUID.randomUUID().toString();
        Mockito.when(cryptoPlugin1.getKeyProviderTypes()).thenReturn(Sets.newHashSet(pluginType1));
        CryptoManager.Factory factory1 = Mockito.mock(CryptoManager.Factory.class);
        Mockito.when(cryptoPlugin1.createClientFactory(Mockito.any())).thenReturn(factory1);
        cryptoPlugins.add(cryptoPlugin1);

        CryptoPlugin cryptoPlugin2 = Mockito.mock(CryptoPlugin.class);
        String pluginType2 = UUID.randomUUID().toString();
        Mockito.when(cryptoPlugin2.getKeyProviderTypes()).thenReturn(Sets.newHashSet(pluginType2));
        CryptoManager.Factory factory2 = Mockito.mock(CryptoManager.Factory.class);
        Mockito.when(cryptoPlugin2.createClientFactory(Mockito.any())).thenReturn(factory2);
        cryptoPlugins.add(cryptoPlugin2);
        CryptoManagerRegistry.initRegistry(cryptoPlugins);

        CryptoManager.Factory factory = CryptoManagerRegistry.getCryptoManagerFactory(pluginType1);
        assertNotNull(factory);
        assertEquals(factory1, factory);

        factory = CryptoManagerRegistry.getCryptoManagerFactory(pluginType2);
        assertNotNull(factory);
        assertEquals(factory2, factory);
    }

    public void testCryptoManagerMissing() {
        CryptoManagerRegistry.registry = new SetOnce<>();
        CryptoManagerRegistry.initRegistry(new ArrayList<>());
        String pluginName = UUID.randomUUID().toString();
        String pluginType = UUID.randomUUID().toString();
        CryptoMetadata cryptoMetadata = new CryptoMetadata(pluginName, pluginType, Settings.EMPTY);
        expectThrows(CryptoRegistryException.class, () -> CryptoManagerRegistry.fetchCryptoManager(cryptoMetadata));
    }

    public void testCryptoManagerCreationFailure() {
        CryptoManagerRegistry.registry = new SetOnce<>();
        List<CryptoPlugin> cryptoPlugins = new ArrayList<>();
        CryptoPlugin cryptoPlugin1 = Mockito.mock(CryptoPlugin.class);

        String pluginType = UUID.randomUUID().toString();
        Mockito.when(cryptoPlugin1.getKeyProviderTypes()).thenReturn(Sets.newHashSet(pluginType));
        CryptoManager.Factory factory1 = Mockito.mock(CryptoManager.Factory.class);
        Mockito.when(cryptoPlugin1.createClientFactory(Mockito.any())).thenReturn(factory1);
        cryptoPlugins.add(cryptoPlugin1);

        CryptoManagerRegistry.initRegistry(cryptoPlugins);
        String pluginName = UUID.randomUUID().toString();
        CryptoMetadata cryptoMetadata = new CryptoMetadata(pluginName, pluginType, Settings.EMPTY);
        Mockito.when(factory1.create(Mockito.any(), Mockito.anyString())).thenThrow(new RuntimeException("Injected failure"));
        expectThrows(CryptoRegistryException.class, () -> CryptoManagerRegistry.fetchCryptoManager(cryptoMetadata));
    }

    public void testCryptoManagerCreationSuccess() {
        CryptoManagerRegistry.registry = new SetOnce<>();
        List<CryptoPlugin> cryptoPlugins = new ArrayList<>();
        CryptoPlugin cryptoPlugin1 = Mockito.mock(CryptoPlugin.class);
        cryptoPlugins.add(cryptoPlugin1);

        String pluginType = UUID.randomUUID().toString();
        String pluginTypeB = UUID.randomUUID().toString();
        Mockito.when(cryptoPlugin1.getKeyProviderTypes()).thenReturn(Sets.newHashSet(pluginType, pluginTypeB));

        CryptoManager.Factory factory1 = Mockito.mock(CryptoManager.Factory.class);
        Mockito.when(cryptoPlugin1.createClientFactory(ArgumentMatchers.matches(pluginType))).thenReturn(factory1);
        CryptoManager cryptoManager = Mockito.mock(CryptoManager.class);
        Mockito.when(factory1.create(Mockito.any(), Mockito.anyString())).thenReturn(cryptoManager);

        CryptoManager.Factory factory2 = Mockito.mock(CryptoManager.Factory.class);
        Mockito.when(cryptoPlugin1.createClientFactory(ArgumentMatchers.matches(pluginTypeB))).thenReturn(factory2);
        CryptoManager cryptoManagerB = Mockito.mock(CryptoManager.class);
        Mockito.when(factory2.create(Mockito.any(), Mockito.anyString())).thenReturn(cryptoManagerB);

        CryptoManagerRegistry.initRegistry(cryptoPlugins);

        String pluginName1 = UUID.randomUUID().toString();
        CryptoMetadata cryptoMetadata = new CryptoMetadata(pluginName1, pluginType, Settings.EMPTY);
        CryptoManager createdCryptoManager1 = CryptoManagerRegistry.fetchCryptoManager(cryptoMetadata);
        assertNotNull(createdCryptoManager1);
        assertEquals(cryptoManager, createdCryptoManager1);
        Mockito.when(factory1.create(Mockito.any(), Mockito.anyString())).thenReturn(Mockito.mock(CryptoManager.class));
        String pluginName2 = UUID.randomUUID().toString();
        CryptoManager createdCryptoManager2 = CryptoManagerRegistry.fetchCryptoManager(new CryptoMetadata(pluginName2, pluginType, Settings.EMPTY));
        assertNotNull(createdCryptoManager2);
        assertNotEquals(createdCryptoManager1, createdCryptoManager2);
        CryptoManager createdCryptoManager3 = CryptoManagerRegistry.fetchCryptoManager(new CryptoMetadata(pluginName2, pluginType, Settings.EMPTY));
        assertNotNull(createdCryptoManager3);
        assertEquals(createdCryptoManager2, createdCryptoManager3);


        CryptoManager createdCryptoMgrNewType = CryptoManagerRegistry.fetchCryptoManager(new CryptoMetadata(pluginName1, pluginTypeB, Settings.EMPTY));
        assertNotNull(createdCryptoMgrNewType);
        assertNotEquals(createdCryptoManager1, createdCryptoMgrNewType);
        assertNotEquals(createdCryptoManager2, createdCryptoMgrNewType);
        assertNotEquals(createdCryptoManager3, createdCryptoMgrNewType);
    }
}
