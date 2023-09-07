/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.encryption;

import org.opensearch.common.crypto.CryptoHandler;
import org.opensearch.common.crypto.MasterKeyProvider;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;

import com.amazonaws.encryptionsdk.caching.CachingCryptoMaterialsManager;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CryptoModulePluginTests extends OpenSearchTestCase {

    private final CryptoModulePlugin cryptoModulePlugin = new CryptoModulePlugin();

    public void testGetOrCreateCryptoHandler() {
        MasterKeyProvider mockKeyProvider = mock(MasterKeyProvider.class);
        when(mockKeyProvider.getEncryptionContext()).thenReturn(Collections.emptyMap());

        CryptoHandler<?, ?> cryptoHandler = cryptoModulePlugin.getOrCreateCryptoHandler(
            mockKeyProvider,
            "keyProviderName",
            "keyProviderType",
            () -> {}
        );

        assertNotNull(cryptoHandler);
    }

    public void testCreateCryptoProvider() {
        CachingCryptoMaterialsManager mockMaterialsManager = mock(CachingCryptoMaterialsManager.class);
        MasterKeyProvider mockKeyProvider = mock(MasterKeyProvider.class);
        when(mockKeyProvider.getEncryptionContext()).thenReturn(Collections.emptyMap());

        CryptoHandler<?, ?> cryptoHandler = cryptoModulePlugin.createCryptoHandler(
            "ALG_AES_256_GCM_IV12_TAG16_HKDF_SHA256",
            mockMaterialsManager,
            mockKeyProvider,
            () -> {}
        );

        assertNotNull(cryptoHandler);
    }

    public void testCreateMaterialsManager() {
        MasterKeyProvider mockKeyProvider = mock(MasterKeyProvider.class);
        when(mockKeyProvider.getEncryptionContext()).thenReturn(Collections.emptyMap());

        CachingCryptoMaterialsManager materialsManager = cryptoModulePlugin.createMaterialsManager(
            mockKeyProvider,
            "keyProviderName",
            "ALG_AES_256_GCM_HKDF_SHA512_COMMIT_KEY_ECDSA_P384"
        );

        assertNotNull(materialsManager);
    }
}
