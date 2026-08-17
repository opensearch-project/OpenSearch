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
 * <p>Both engines keep the same Lucene representation, and the composite engine adds a columnar copy:
 *
 * <table>
 *   <caption>Where a flat_object field's data lands</caption>
 *   <tr><th></th><th>internal engine (Lucene)</th><th>composite engine (parquet + lucene)</th></tr>
 *   <tr><td>Lucene inverted index</td>
 *       <td>{@code field}, {@code field._value}, {@code field._valueAndPath}</td>
 *       <td>the same three fields</td></tr>
 *   <tr><td>Lucene doc values</td><td>SORTED_SET on all three</td>
 *       <td>none — the primary format owns columnar storage</td></tr>
 *   <tr><td>Parquet</td><td>n/a</td><td>one {@code MAP<utf8, utf8>} column</td></tr>
 * </table>
 *
 * <p>That split comes from capability assignment. A flat_object field requests
 * {@code FULL_TEXT_SEARCH} (it is searchable) and {@code COLUMNAR_STORAGE} (it has doc values); the
 * parquet primary claims only {@code COLUMNAR_STORAGE}, leaving the inverted index to the lucene
 * secondary — exactly how {@code keyword} and {@code text} are split, which is what keeps them
 * searchable on a composite index. An earlier revision had the parquet field claim both, which starved
 * Lucene of the field entirely and left it searchable by neither format; these tests exist to keep
 * that from regressing.
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
     * The composite engine indexes the same three Lucene fields, so basic searches keep working: the
     * parquet primary claims only {@code COLUMNAR_STORAGE} for a flat_object, leaving
     * {@code FULL_TEXT_SEARCH} to the lucene secondary — the same split that keeps keyword and text
     * searchable. The Parquet MAP column is the columnar copy alongside it, not a replacement.
     */
    public void testCompositeEngineAlsoIndexesFlatObjectSubFieldsInLucene() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(1);
        createAndIndex(COMPOSITE_INDEX, compositeSettings());

        Set<String> fields = luceneFields(COMPOSITE_INDEX);
        assertTrue("flat_object parent must be indexed: " + fields, fields.contains("LogAttributes"));
        assertTrue("_value sub-field must be indexed: " + fields, fields.contains("LogAttributes._value"));
        assertTrue("_valueAndPath sub-field must be indexed: " + fields, fields.contains("LogAttributes._valueAndPath"));

        // The capability split is per field, not per index: keyword/text also still reach Lucene.
        assertTrue("keyword must still be indexed in lucene: " + fields, fields.contains("ServiceName"));
        assertTrue("text must still be indexed in lucene: " + fields, fields.contains("Body"));
    }

    /**
     * On the internal engine the leaves are searchable through the sub-fields the mapper writes: a
     * dotted-path term query rewrites to a {@code <field>.<path>=<value>} term on
     * {@code _valueAndPath}, and a query on the field itself to the bare value on {@code _value}. The
     * composite engine now writes those same terms, which is what the previous test asserts.
     */
    public void testInternalEngineServesBasicSearchesOnFlatObject() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(1);
        createAndIndex(LUCENE_INDEX, luceneSettings());

        assertEquals(
            "dotted-path term query must match the flat_object leaf",
            1L,
            hits(LUCENE_INDEX, QueryBuilders.termQuery("LogAttributes.http.status", "500"))
        );
        assertEquals(
            "query on the field itself must match a bare leaf value",
            1L,
            hits(LUCENE_INDEX, QueryBuilders.termQuery("LogAttributes", "GET"))
        );
        assertEquals(
            "a value that was never indexed must not match",
            0L,
            hits(LUCENE_INDEX, QueryBuilders.termQuery("LogAttributes.http.status", "404"))
        );
        assertEquals("exists query must see the flat_object field", 1L, hits(LUCENE_INDEX, QueryBuilders.existsQuery("LogAttributes")));
    }

    /**
     * flat_object is now searchable on a composite index to exactly the same degree as {@code keyword}
     * — which is the meaningful parity bar, because the transport {@code _search} action is not
     * supported on a composite index for <em>any</em> field type: {@code IndexShard} refuses to apply
     * it to a {@code DataFormatAwareEngine}, and queries are instead served through the analytics
     * engine (PPL / the DSL query executor), which the {@code analytics-engine-rest} QA suite covers.
     *
     * <p>Asserting the two field types fail identically keeps this engine-wide limitation from being
     * mistaken for a flat_object gap: before the capability split was corrected, flat_object had no
     * Lucene terms at all, so it could never become searchable even once that limitation is lifted.
     */
    public void testFlatObjectAndKeywordAreEquallySearchableOnCompositeEngine() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(1);
        createAndIndex(COMPOSITE_INDEX, compositeSettings());

        Exception flatObjectFailure = expectThrows(
            Exception.class,
            () -> hits(COMPOSITE_INDEX, QueryBuilders.termQuery("LogAttributes.http.status", "500"))
        );
        Exception keywordFailure = expectThrows(
            Exception.class,
            () -> hits(COMPOSITE_INDEX, QueryBuilders.termQuery("ServiceName", "checkout"))
        );
        for (Exception failure : List.of(flatObjectFailure, keywordFailure)) {
            assertTrue(
                "transport _search must be refused for the engine, not for the field: " + failure,
                failure.toString().contains("DataFormatAwareEngine")
            );
        }
    }

    private long hits(String index, org.opensearch.index.query.QueryBuilder query) {
        return client().prepareSearch(index).setQuery(query).get().getHits().getTotalHits().value();
    }
}
