/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.arrow.allocator.ArrowBasePlugin;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.Collection;

/**
 * Integration tests for field validation in the composite data format engine.
 * Validates that system-managed metadata fields reject external input and that
 * multi-value fields are rejected for columnar formats.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class CompositeFieldValidationIT extends OpenSearchIntegTestCase {

    private static final String INDEX_NAME = "test-field-validation";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(
            ArrowBasePlugin.class,
            ParquetDataFormatPlugin.class,
            CompositeDataFormatPlugin.class,
            LucenePlugin.class,
            DataFusionPlugin.class
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            .build();
    }

    private Settings compositeIndexSettings() {
        return Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats", "lucene")
            .build();
    }

    public void testRowIdFieldRejectedInDocument() throws Exception {
        client().admin()
            .indices()
            .prepareCreate(INDEX_NAME)
            .setSettings(compositeIndexSettings())
            .setMapping("name", "type=keyword", "age", "type=integer")
            .get();
        ensureGreen(INDEX_NAME);

        BulkResponse bulkResponse = client().prepareBulk()
            .add(new IndexRequest(INDEX_NAME).source(XContentType.JSON, "__row_id__", 999, "name", "test", "age", 25))
            .get();

        assertTrue("Expected bulk to have failures for __row_id__ field", bulkResponse.hasFailures());
        BulkItemResponse item = bulkResponse.getItems()[0];
        assertTrue(item.isFailed());
        assertTrue(item.getFailureMessage().contains("metadata field and cannot be added inside a document"));
    }

    public void testMultipleValuesForSameFieldRejected() throws Exception {
        client().admin()
            .indices()
            .prepareCreate(INDEX_NAME)
            .setSettings(compositeIndexSettings())
            .setMapping("name", "type=keyword", "age", "type=integer")
            .get();
        ensureGreen(INDEX_NAME);

        BulkResponse bulkResponse = client().prepareBulk()
            .add(new IndexRequest(INDEX_NAME).source("{\"age\": [10, 20], \"name\": \"test\"}", XContentType.JSON))
            .get();

        assertTrue("Expected rejection for multi-value field", bulkResponse.hasFailures());
        BulkItemResponse item = bulkResponse.getItems()[0];
        assertTrue(item.isFailed());
        assertTrue(item.getFailureMessage().contains("multiple values"));
    }
}
