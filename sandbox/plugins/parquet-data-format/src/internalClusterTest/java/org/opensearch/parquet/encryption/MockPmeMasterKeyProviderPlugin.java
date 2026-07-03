/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.encryption;

import org.opensearch.cluster.metadata.CryptoMetadata;
import org.opensearch.common.Randomness;
import org.opensearch.common.crypto.DataKeyPair;
import org.opensearch.common.crypto.MasterKeyProvider;
import org.opensearch.plugins.CryptoKeyProviderPlugin;
import org.opensearch.plugins.Plugin;

import static org.mockito.AdditionalAnswers.returnsFirstArg;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * TODO: Mock KMS for testing; should be also consolidated with testing approach used by Lucene encryption plugin.
 *
 * Test plugin that provides a mock {@link MasterKeyProvider} for PME integration tests.
 *
 * <p>Analogous to {@code MockCryptoKeyProviderPlugin} in the Lucene storage-encryption plugin.
 * The mock provider uses random but fixed-per-instance 32-byte keys and implements
 * {@code decryptKey} as an identity function (returns the first argument unchanged), so
 * key material round-trips through the keyfile without a real KMS.
 *
 * <p>Registers under the type {@value #TYPE}. Integration tests configure their indices with
 * {@code index.store.parquet.crypto.key_provider_type = "mock-pme"}.
 */
public class MockPmeMasterKeyProviderPlugin extends Plugin implements CryptoKeyProviderPlugin {

    public static final String TYPE = "mock-pme";

    @Override
    public MasterKeyProvider createKeyProvider(CryptoMetadata cryptoMetadata) {
        MasterKeyProvider provider = mock(MasterKeyProvider.class);

        byte[] rawKey = new byte[32];
        byte[] encryptedKey = new byte[32];
        Randomness.get().nextBytes(rawKey);
        Randomness.get().nextBytes(encryptedKey);

        when(provider.generateDataPair()).thenReturn(new DataKeyPair(rawKey, encryptedKey));
        // Identity decrypt: the keyfile stores "encryptedKey", decryptKey returns it as-is.
        // This means every call to decryptKey returns the same bytes that were written,
        // satisfying the 32-byte length check in PmeKeyfileManager without a real KMS.
        when(provider.decryptKey(any(byte[].class))).then(returnsFirstArg());
        return provider;
    }

    @Override
    public String type() {
        return TYPE;
    }
}

