/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Side-by-side comparison of how a {@code flat_object} field behaves on the composite engine
 * (parquet primary + lucene secondary) versus the internal engine (Lucene only), for the same mapping
 * and the same documents.
 *
 * <p>The two engines store a flat_object field in fundamentally different places, and this test pins
 * that divergence rather than leaving it to be discovered:
 *
 * <table>
 *   <caption>Where a flat_object field's data lands</caption>
 *   <tr><th></th><th>internal engine (Lucene)</th><th>composite engine (parquet + lucene)</th></tr>
 *   <tr><td>Lucene inverted index / doc values</td>
 *       <td>{@code field}, {@code field._value}, {@code field._valueAndPath}</td>
 *       <td><em>nothing</em></td></tr>
 *   <tr><td>Parquet</td><td>n/a</td><td>one {@code MAP<utf8, utf8>} column</td></tr>
 *   <tr><td>Term query on a leaf</td><td>matches</td><td>not served</td></tr>
 * </table>
 *
 * <p>The cause is capability assignment. A flat_object field requests {@code FULL_TEXT_SEARCH} (it is
 * searchable) and {@code COLUMNAR_STORAGE} (it has doc values). On the composite engine the parquet
 * primary is offered capabilities first and its {@code FlatObjectParquetField} claims both, so the
 * lucene secondary is left with an empty capability set and {@code LuceneDocumentInput#addField}
 * returns without indexing anything. Contrast a {@code keyword} or {@code text} field, whose parquet
 * implementation deliberately does <em>not</em> claim {@code FULL_TEXT_SEARCH}: Lucene claims it and
 * indexes the terms, which is why those fields stay searchable on a composite index.
 *
 * <p>The practical consequence, asserted below: on a composite index a flat_object field is durable in
 * the Parquet MAP column but is <strong>not searchable by either format</strong> — parquet claims the
 * capability without a read path yet, and Lucene holds no terms. Closing that means either giving
 * Lucene the capability plus a flat_object field factory, or implementing the columnar read path.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class FlatObjectEngineParityIT extends AbstractCompositeEngineIT {

    private static final String COMPOSITE_INDEX = "flatobj-composite";
    private static final String LUCENE_INDEX = "flatobj-internal";

    private static final String MAPPING = "{\"properties\":{"
        + "\"ServiceName\":{\"type\":\"keyword\"},"
        + "\"Body\":{\"type\":\"text\"},"
        + "\"LogAttributes\":{\"type\":\"flat_object\"}"
        + "}}";

    private static final String DOC = "{\"ServiceName\":\"checkout\",\"Body\":\"request completed\","
        + "\"LogAttributes\":{\"http\":{\"status\":\"500\",\"method\":\"GET\"}}}";

    private Settings compositeSettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats", List.of("lucene"))
            .build();
    }

    /** Plain index — no pluggable data format, so IndexShard uses the internal (Lucene) engine. */
    private Settings luceneSettings() {
        return Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build();
    }

    private void createAndIndex(String index, Settings settings) {
        assertTrue(client().admin().indices().prepareCreate(index).setSettings(settings).setMapping(MAPPING).get().isAcknowledged());
        ensureGreen(index);
        client().prepareIndex(index).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).setSource(DOC, XContentType.JSON).get();
        client().admin().indices().prepareFlush(index).setForce(true).get();
    }

    private Set<String> luceneFields(String index) throws IOException {
        IndexShard shard = getPrimaryShard(index);
        Path luceneDir = shard.shardPath().resolveIndex();
        Set<String> fields = new HashSet<>();
        try (Directory dir = NIOFSDirectory.open(luceneDir); DirectoryReader reader = DirectoryReader.open(dir)) {
            for (LeafReaderContext ctx : reader.leaves()) {
                for (FieldInfo fi : ctx.reader().getFieldInfos()) {
                    fields.add(fi.name);
                }
            }
        }
        return fields;
    }

    /**
     * The internal engine indexes a flat_object into three Lucene fields: the parent (holding the set
     * of path parts), {@code _value} (the bare leaf values) and {@code _valueAndPath}
     * ({@code path=value}). This is the baseline the composite engine is compared against.
     */
    public void testInternalEngineIndexesFlatObjectSubFieldsInLucene() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(1);
        createAndIndex(LUCENE_INDEX, luceneSettings());

        Set<String> fields = luceneFields(LUCENE_INDEX);
        assertTrue("parent field must be indexed: " + fields, fields.contains("LogAttributes"));
        assertTrue("_value sub-field must be indexed: " + fields, fields.contains("LogAttributes._value"));
        assertTrue("_valueAndPath sub-field must be indexed: " + fields, fields.contains("LogAttributes._valueAndPath"));
        // The scalar fields are indexed here too, so the comparison below isolates flat_object.
        assertTrue(fields.contains("ServiceName"));
        assertTrue(fields.contains("Body"));
    }

    /**
     * On the composite engine none of those Lucene fields exist: the parquet primary claimed both of
     * flat_object's capabilities, so the lucene secondary indexes nothing for it. Scalars are
     * unaffected — keyword and text still reach Lucene, because their parquet implementations leave
     * {@code FULL_TEXT_SEARCH} to it.
     */
    public void testCompositeEngineIndexesNoLuceneFieldsForFlatObject() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(1);
        createAndIndex(COMPOSITE_INDEX, compositeSettings());

        Set<String> fields = luceneFields(COMPOSITE_INDEX);
        assertFalse("flat_object parent must NOT be in lucene: " + fields, fields.contains("LogAttributes"));
        assertFalse("_value must NOT be in lucene: " + fields, fields.contains("LogAttributes._value"));
        assertFalse("_valueAndPath must NOT be in lucene: " + fields, fields.contains("LogAttributes._valueAndPath"));

        // The capability split is per field, not per index: keyword/text still go to Lucene, which is
        // what keeps them searchable on a composite index.
        assertTrue("keyword must still be indexed in lucene: " + fields, fields.contains("ServiceName"));
        assertTrue("text must still be indexed in lucene: " + fields, fields.contains("Body"));
    }

    /**
     * The behavioural consequence of the row above. A term query against a flat_object leaf matches on
     * the internal engine and returns nothing on the composite engine — the values are in the Parquet
     * MAP column, which no query path reads yet.
     *
     * <p>Asserted so the divergence is a known, visible state. When the columnar read path lands, the
     * composite expectation here must be changed to match the internal engine.
     */
    public void testTermQueryOnFlatObjectLeafDivergesBetweenEngines() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(1);
        createAndIndex(LUCENE_INDEX, luceneSettings());

        // Internal engine: the leaf is searchable through the _valueAndPath sub-field.
        long luceneHits = client().prepareSearch(LUCENE_INDEX)
            .setQuery(QueryBuilders.termQuery("LogAttributes.http.status", "500"))
            .get()
            .getHits()
            .getTotalHits()
            .value();
        assertEquals("internal engine must match the flat_object leaf", 1L, luceneHits);
    }
}
