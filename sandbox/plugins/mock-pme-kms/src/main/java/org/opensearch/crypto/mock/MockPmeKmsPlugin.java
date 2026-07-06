/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.crypto.mock;

import org.opensearch.cluster.metadata.CryptoMetadata;
import org.opensearch.common.crypto.MasterKeyProvider;
import org.opensearch.plugins.CryptoKeyProviderPlugin;
import org.opensearch.plugins.Plugin;

/**
 * TODO: Mock KMS for testing; should be also consolidated with testing approach used by Lucene encryption plugin.
 *
 * OpenSearch plugin that registers a dummy KMS provider for PME end-to-end testing.
 *
 * <p>Installs the {@value #TYPE} key provider type. Configure an encrypted Parquet index with:
 * <pre>
 *   "index.store.parquet.crypto.key_provider":      "any-name"
 *   "index.store.parquet.crypto.key_provider_type": "mock-pme"
 * </pre>
 *
 * <p><strong>NOT for production use.</strong> The provider performs no real key-wrapping;
 * decryption is an identity function. Install only in development or CI clusters.
 */
public class MockPmeKmsPlugin extends Plugin implements CryptoKeyProviderPlugin {

    /** Key provider type string used in index settings. */
    public static final String TYPE = "mock-pme";

    @Override
    public MasterKeyProvider createKeyProvider(CryptoMetadata cryptoMetadata) {
        return new MockPmeMasterKeyProvider(cryptoMetadata);
    }

    @Override
    public String type() {
        return TYPE;
    }
}

