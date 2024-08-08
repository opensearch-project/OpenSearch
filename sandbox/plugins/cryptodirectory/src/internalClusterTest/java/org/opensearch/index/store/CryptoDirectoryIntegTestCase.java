/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.store;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.reindex.ReindexAction;
import org.opensearch.index.reindex.ReindexModulePlugin;
import org.opensearch.index.reindex.ReindexRequestBuilder;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.Collection;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.hamcrest.Matchers.is;

public class CryptoDirectoryIntegTestCase extends OpenSearchIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(
            CryptoDirectoryPlugin.class,
            MockCryptoKeyProviderPlugin.class,
            MockCryptoPlugin.class,
            ReindexModulePlugin.class
        );
    }

    @Override
    public Settings indexSettings() {
        return Settings.builder()
            .put(super.indexSettings())
            .put("index.store.type", "cryptofs")
            .put("index.store.kms.type", "dummy")
            .put(SETTING_NUMBER_OF_SHARDS, 1)
            .put(SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
    }

    public void testReindex() {
        // Create an index and index some documents
        createIndex("test");
        createIndex("test_copy");
        long nbDocs = randomIntBetween(10, 1000);
        for (long i = 0; i < nbDocs; i++) {
            index("test", "doc", "" + i, "foo", "bar");
        }
        refresh();
        SearchResponse response = client().prepareSearch("test").get();
        assertThat(response.getHits().getTotalHits().value, is(nbDocs));

        // Reindex
        reindex().source("test").destination("test_copy").refresh(true).get();
        SearchResponse copy_response = client().prepareSearch("test_copy").get();
        assertThat(copy_response.getHits().getTotalHits().value, is(nbDocs));

    }

    ReindexRequestBuilder reindex() {
        return new ReindexRequestBuilder(client(), ReindexAction.INSTANCE);
    }

}
