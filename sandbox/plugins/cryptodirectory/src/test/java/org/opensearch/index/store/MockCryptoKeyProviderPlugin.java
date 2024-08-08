/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.store;

import org.opensearch.cluster.metadata.CryptoMetadata;
import org.opensearch.common.Randomness;
import org.opensearch.common.crypto.DataKeyPair;
import org.opensearch.common.crypto.MasterKeyProvider;
import org.opensearch.plugins.CryptoKeyProviderPlugin;
import org.opensearch.plugins.Plugin;

import static org.mockito.AdditionalAnswers.returnsFirstArg;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Some tests rely on the keyword tokenizer, but this tokenizer isn't part of lucene-core and therefor not available
 * in some modules. What this test plugin does, is use the mock tokenizer and advertise that as the keyword tokenizer.
 * <p>
 * Most tests that need this test plugin use normalizers. When normalizers are constructed they try to resolve the
 * keyword tokenizer, but if the keyword tokenizer isn't available then constructing normalizers will fail.
 */
public class MockCryptoKeyProviderPlugin extends Plugin implements CryptoKeyProviderPlugin {

    @Override
    public MasterKeyProvider createKeyProvider(CryptoMetadata cryptoMetadata) {
        MasterKeyProvider keyProvider = mock(MasterKeyProvider.class);
        byte[] rawKey = new byte[32];
        byte[] encryptedKey = new byte[32];
        java.util.Random rnd = Randomness.get();
        rnd.nextBytes(rawKey);
        rnd.nextBytes(encryptedKey);
        DataKeyPair dataKeyPair = new DataKeyPair(rawKey, encryptedKey);
        when(keyProvider.generateDataPair()).thenReturn(dataKeyPair);
        when(keyProvider.decryptKey(any(byte[].class))).then(returnsFirstArg());
        return keyProvider;
    }

    @Override
    public String type() {
        return "dummy";
    }
}
