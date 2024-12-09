/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.store;

import org.opensearch.common.crypto.CryptoHandler;
import org.opensearch.common.crypto.MasterKeyProvider;
import org.opensearch.plugins.CryptoPlugin;
import org.opensearch.plugins.Plugin;

import static org.mockito.Mockito.mock;

/**
 * Some tests rely on the keyword tokenizer, but this tokenizer isn't part of lucene-core and therefor not available
 * in some modules. What this test plugin does, is use the mock tokenizer and advertise that as the keyword tokenizer.
 * <p>
 * Most tests that need this test plugin use normalizers. When normalizers are constructed they try to resolve the
 * keyword tokenizer, but if the keyword tokenizer isn't available then constructing normalizers will fail.
 */
public class MockCryptoPlugin extends Plugin implements CryptoPlugin<Object, Object> {

    @Override
    @SuppressWarnings("unchecked")
    public CryptoHandler<Object, Object> getOrCreateCryptoHandler(
        MasterKeyProvider keyProvider,
        String keyProviderName,
        String keyProviderType,
        Runnable onClose
    ) {
        CryptoHandler<Object, Object> handler = (CryptoHandler<Object, Object>) mock(CryptoHandler.class);
        return handler;
    }
}
