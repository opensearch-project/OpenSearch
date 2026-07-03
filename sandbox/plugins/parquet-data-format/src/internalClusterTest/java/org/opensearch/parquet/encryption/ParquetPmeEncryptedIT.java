/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.encryption;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.parquet.engine.ParquetDataFormat;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.Collection;

import static org.hamcrest.Matchers.is;

/**
 * End-to-end integration tests for Parquet Modular Encryption (PME).
 *
 * <p>Analogous to {@code CryptoDirectoryIntegTestCases} in the Lucene storage-encryption
 * plugin. A {@link MockPmeMasterKeyProviderPlugin} is loaded as a node plugin so that the
 * {@link org.opensearch.crypto.CryptoHandlerRegistry} can resolve the key provider without
 * a real KMS. Every index is created with:
 * <ul>
 *   <li>{@code index.pluggable.dataformat = "parquet"} — activates the Parquet data format engine</li>
 *   <li>{@code index.store.parquet.crypto.key_provider = "test"} — triggers PME context creation</li>
 *   <li>{@code index.store.parquet.crypto.key_provider_type = "mock-pme"} — selects the mock provider</li>
 * </ul>
 *
 * <p>The tests verify that documents can be indexed and searched transparently through the
 * full PME stack (keyfile creation → key derivation → Parquet write → read).
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class ParquetPmeEncryptedIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(ParquetDataFormatPlugin.class, MockPmeMasterKeyProviderPlugin.class);
    }

    @Override
    protected boolean addMockInternalEngine() {
        // Must return false so the real Parquet engine is used, not OpenSearch's mock engine.
        // This mirrors the same override in CryptoDirectoryIntegTestCases.
        return false;
    }

    /** Index settings shared by all test methods: Parquet format + PME mock provider. */
    private Settings encryptedParquetIndexSettings() {
        return Settings.builder()
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", ParquetDataFormat.PARQUET_DATA_FORMAT_NAME)
            .put("index.store.parquet.crypto.key_provider", "test")
            .put("index.store.parquet.crypto.key_provider_type", MockPmeMasterKeyProviderPlugin.TYPE)
            .put(indexSettings())
            .build();
    }

    /**
     * Verifies that documents indexed into an encrypted Parquet index are searchable.
     * This exercises the full write path (keyfile creation, key derivation, PME footer key
     * injection into parquet-rs) and the full read path (keyfile load, key derivation,
     * footer key resolution for decryption).
     */
    public void testIndexAndSearch() {
        createIndex("pme-test", encryptedParquetIndexSettings());

        long nbDocs = randomIntBetween(1, 50);
        for (long i = 0; i < nbDocs; i++) {
            index("pme-test", "doc", String.valueOf(i), "field", "value_" + i);
        }
        refresh("pme-test");

        SearchResponse response = client().prepareSearch("pme-test").get();
        assertThat(response.getHits().getTotalHits().value(), is(nbDocs));
    }

    /**
     * Verifies that an encrypted Parquet index can be deleted and is then gone.
     * Deletion must also evict the PME cache entries cleanly (no leaked key material).
     */
    public void testDeleteEncryptedIndex() {
        createIndex("pme-delete", encryptedParquetIndexSettings());

        long nbDocs = randomIntBetween(1, 20);
        for (long i = 0; i < nbDocs; i++) {
            index("pme-delete", "doc", String.valueOf(i), "field", "value_" + i);
        }
        refresh("pme-delete");

        SearchResponse before = client().prepareSearch("pme-delete").get();
        assertThat(before.getHits().getTotalHits().value(), is(nbDocs));

        client().admin().indices().prepareDelete("pme-delete").get();

        expectThrows(Exception.class, () -> client().prepareSearch("pme-delete").get());
    }

    /**
     * Verifies that multiple separate encrypted indices each get their own keyfile and
     * are independently readable. Each index uses the same mock provider but generates
     * a distinct data key (different {@code generateDataPair()} call per index).
     */
    public void testTwoIndependentEncryptedIndices() {
        createIndex("pme-alpha", encryptedParquetIndexSettings());
        createIndex("pme-beta", encryptedParquetIndexSettings());

        for (long i = 0; i < 5; i++) {
            index("pme-alpha", "doc", String.valueOf(i), "color", "red");
            index("pme-beta", "doc", String.valueOf(i), "color", "blue");
        }
        refresh("pme-alpha", "pme-beta");

        SearchResponse alpha = client().prepareSearch("pme-alpha").get();
        SearchResponse beta = client().prepareSearch("pme-beta").get();
        assertThat(alpha.getHits().getTotalHits().value(), is(5L));
        assertThat(beta.getHits().getTotalHits().value(), is(5L));
    }
}






