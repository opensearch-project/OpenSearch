/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.encryption;

import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;

import com.amazonaws.encryptionsdk.caching.CachingCryptoMaterialsManager;
import org.junit.Before;
import org.opensearch.common.crypto.CryptoProvider;
import org.opensearch.common.crypto.MasterKeyProvider;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;

public class CryptoManagerFactoryTests extends OpenSearchTestCase {

    private CryptoManagerFactory cryptoManagerFactory;

    @Before
    public void setup() {
        cryptoManagerFactory = new CryptoManagerFactory("ALG_AES_256_GCM_HKDF_SHA512_COMMIT_KEY", TimeValue.timeValueDays(2), 10);
    }

    public void testGetOrCreateCryptoManager() {
        MasterKeyProvider mockKeyProvider = mock(MasterKeyProvider.class);
        when(mockKeyProvider.getEncryptionContext()).thenReturn(Collections.emptyMap());

        CryptoManager cryptoManager = cryptoManagerFactory.getOrCreateCryptoManager(
            mockKeyProvider,
            "keyProviderName",
            "keyProviderType",
            () -> {}
        );

        assertNotNull(cryptoManager);
    }

    public void testCreateCryptoProvider() {
        CachingCryptoMaterialsManager mockMaterialsManager = mock(CachingCryptoMaterialsManager.class);
        MasterKeyProvider mockKeyProvider = mock(MasterKeyProvider.class);
        when(mockKeyProvider.getEncryptionContext()).thenReturn(Collections.emptyMap());

        CryptoProvider cryptoProvider = cryptoManagerFactory.createCryptoProvider(
            "ALG_AES_256_GCM_HKDF_SHA512_COMMIT_KEY",
            mockMaterialsManager,
            mockKeyProvider
        );

        assertNotNull(cryptoProvider);
    }

    public void testCreateMaterialsManager() {
        MasterKeyProvider mockKeyProvider = mock(MasterKeyProvider.class);
        when(mockKeyProvider.getEncryptionContext()).thenReturn(Collections.emptyMap());

        CachingCryptoMaterialsManager materialsManager = cryptoManagerFactory.createMaterialsManager(
            mockKeyProvider,
            "keyProviderName",
            "ALG_AES_256_GCM_HKDF_SHA512_COMMIT_KEY"
        );

        assertNotNull(materialsManager);
    }

    public void testCreateCryptoManager() {
        CryptoProvider mockCryptoProvider = mock(CryptoProvider.class);
        CryptoManager cryptoManager = cryptoManagerFactory.createCryptoManager(
            mockCryptoProvider,
            "keyProviderName",
            "keyProviderType",
            null
        );
        assertNotNull(cryptoManager);
    }

    public void testUnsupportedAlgorithm() {
        expectThrows(IllegalArgumentException.class, () -> new CryptoManagerFactory("Unsupported_algo", TimeValue.timeValueDays(2), 10));

        expectThrows(
            IllegalArgumentException.class,
            () -> cryptoManagerFactory.createCryptoProvider(
                "Unsupported_algo",
                mock(CachingCryptoMaterialsManager.class),
                mock(MasterKeyProvider.class)
            )
        );
    }
}
