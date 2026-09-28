/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.opensearch.ExceptionsHelper;
import org.opensearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.arrow.allocator.ArrowBasePlugin;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.cluster.metadata.ComposableIndexTemplate;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Template;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.mapper.MapperParsingException;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.parquet.bridge.RustBridge;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;

/**
 * Verifies that a document indexed before its index exists (auto-create) infers
 * {@code multi_value: true} for fields whose first value is a JSON array, with adaptive promotion
 * left at its default (disabled). The index template selects the composite Parquet format and
 * routes dynamic strings to {@code keyword}, mirroring a typical log-analytics template.
 * <p>
 * Requires JDK 25 and sandbox enabled. Run with:
 * ./gradlew :sandbox:plugins:composite-engine:internalClusterTest \\
 * --tests "*.CompositeDynamicArrayInferenceIT" \\
 * -Dsandbox.enabled=true
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class CompositeDynamicArrayInferenceIT extends OpenSearchIntegTestCase {

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
        // Deliberately leaves PARQUET_MULTI_VALUE_AUTO_PROMOTION_EXPERIMENTAL_FLAG at its default (false).
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            .build();
    }

    public void testFirstDocumentArrayAutoCreatesIndexWithMultiValueTrue() throws Exception {
        String indexName = "logs-array-first";
        putLogsTemplate("logs-array-first-template", indexName, null);

        IndexResponse first = client().prepareIndex(indexName).setSource("tags", List.of("prod", "error"), "host", "h1").get();
        assertEquals(RestStatus.CREATED, first.status());
        ensureGreen(indexName);

        assertEquals(Boolean.TRUE, clusterStateFieldMapping(indexName, "tags").get("multi_value"));
        assertEquals("keyword", clusterStateFieldMapping(indexName, "tags").get("type"));
        // A scalar sibling in the same document is not inferred multi-valued and stays unserialized.
        assertFalse(clusterStateFieldMapping(indexName, "host").containsKey("multi_value"));

        // The LIST field accepts scalars and arrays in later documents without any further mapping change.
        long mappingVersion = getClusterState().metadata().index(indexName).getMappingVersion();
        assertEquals(RestStatus.CREATED, client().prepareIndex(indexName).setSource("tags", "solo", "host", "h2").get().status());
        assertEquals(
            RestStatus.CREATED,
            client().prepareIndex(indexName).setSource("tags", List.of("a", "b", "c"), "host", "h3").get().status()
        );
        assertEquals(mappingVersion, getClusterState().metadata().index(indexName).getMappingVersion());

        List<Map<String, Object>> rows = refreshFlushAndReadParquetRows(indexName);
        assertEquals(3, rows.size());
        assertTrue(rows.stream().allMatch(row -> isListColumnValue(row.get("tags"))));
    }

    public void testFirstDocumentSingletonArrayInfersMultiValueTrue() throws Exception {
        String indexName = "logs-singleton-first";
        putLogsTemplate("logs-singleton-first-template", indexName, null);

        assertEquals(RestStatus.CREATED, client().prepareIndex(indexName).setSource("tags", List.of("only")).get().status());
        ensureGreen(indexName);
        assertEquals(Boolean.TRUE, clusterStateFieldMapping(indexName, "tags").get("multi_value"));

        // Without inference a one-element array would have landed scalar and this document would be rejected.
        assertEquals(RestStatus.CREATED, client().prepareIndex(indexName).setSource("tags", List.of("a", "b")).get().status());

        List<Map<String, Object>> rows = refreshFlushAndReadParquetRows(indexName);
        assertEquals(2, rows.size());
        assertTrue(rows.stream().allMatch(row -> isListColumnValue(row.get("tags"))));
    }

    public void testFirstDocumentScalarStaysScalarAndLaterArrayIsRejected() throws Exception {
        String indexName = "logs-scalar-first";
        putLogsTemplate("logs-scalar-first-template", indexName, null);

        assertEquals(RestStatus.CREATED, client().prepareIndex(indexName).setSource("tags", "solo").get().status());
        ensureGreen(indexName);
        assertFalse(clusterStateFieldMapping(indexName, "tags").containsKey("multi_value"));

        MapperParsingException error = expectThrows(
            MapperParsingException.class,
            () -> client().prepareIndex(indexName).setSource("tags", List.of("a", "b")).get()
        );
        assertThat(ExceptionsHelper.stackTrace(error), containsString("automatic promotion is disabled"));
        assertFalse(clusterStateFieldMapping(indexName, "tags").containsKey("multi_value"));
    }

    public void testTemplatePinnedScalarIsHonouredForFirstDocumentArray() throws Exception {
        String indexName = "logs-pinned-scalar";
        putLogsTemplate("logs-pinned-scalar-template", indexName, false);

        MapperParsingException error = expectThrows(
            MapperParsingException.class,
            () -> client().prepareIndex(indexName).setSource("tags", List.of("prod", "error")).get()
        );
        assertThat(ExceptionsHelper.stackTrace(error), containsString("locked scalar by [multi_value: false]"));

        // The index was auto-created and the field mapping published from the first parse pass keeps
        // the template's explicit scalar pin; inference never overrides an explicit value.
        assertTrue(indexExists(indexName));
        assertEquals(Boolean.FALSE, clusterStateFieldMapping(indexName, "tags").get("multi_value"));
    }

    // ══════════════════════════════════════════════════════════════════════
    // Helpers
    // ══════════════════════════════════════════════════════════════════════

    /**
     * Installs a composable index template matching {@code indexPattern} that selects the composite
     * Parquet format and maps dynamic strings to {@code keyword}. When {@code templateMultiValue} is
     * non-null the template pins {@code multi_value} explicitly.
     */
    private void putLogsTemplate(String templateName, String indexPattern, Boolean templateMultiValue) throws Exception {
        String multiValueClause = templateMultiValue == null ? "" : ", \"multi_value\": " + templateMultiValue;
        String mappings = "{\"dynamic_templates\": [{\"strings_as_keywords\": {\"match_mapping_type\": \"string\", "
            + "\"mapping\": {\"type\": \"keyword\""
            + multiValueClause
            + "}}}]}";
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats", "lucene")
            .build();
        ComposableIndexTemplate template = new ComposableIndexTemplate(
            List.of(indexPattern),
            new Template(settings, new CompressedXContent(mappings), null),
            null,
            100L,
            null,
            null
        );
        assertTrue(
            client().execute(
                PutComposableIndexTemplateAction.INSTANCE,
                new PutComposableIndexTemplateAction.Request(templateName).indexTemplate(template)
            ).get().isAcknowledged()
        );
    }

    /**
     * RustBridge's test-only JSON renderer emits either a decoded array or an
     * {@code <unsupported:List(...)>} marker for LIST columns, depending on the native build.
     * Either proves the physical Parquet column is LIST rather than a scalar UTF8 column.
     */
    private static boolean isListColumnValue(Object value) {
        return value instanceof List<?> || (value instanceof String text && text.startsWith("<unsupported:List("));
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> clusterStateFieldMapping(String indexName, String fieldName) {
        Map<String, Object> mappingSource = getClusterState().metadata().index(indexName).mapping().sourceAsMap();
        Map<String, Object> properties = (Map<String, Object>) mappingSource.get("properties");
        return properties == null ? null : (Map<String, Object>) properties.get(fieldName);
    }

    private List<Map<String, Object>> refreshFlushAndReadParquetRows(String indexName) throws IOException {
        client().admin().indices().prepareRefresh(indexName).get();
        client().admin().indices().prepareFlush(indexName).setForce(true).setWaitIfOngoing(true).get();
        IndexShard shard = getIndexShard(
            internalCluster().getDataNodeNames().iterator().next(),
            new ShardId(resolveIndex(indexName), 0),
            indexName
        );
        Path parquetDir = shard.shardPath().getDataPath().resolve("parquet");
        assertTrue("Parquet directory should exist", Files.isDirectory(parquetDir));
        GatedCloseable<CatalogSnapshot> snapshot = shard.getCatalogSnapshot();
        try (snapshot) {
            List<Map<String, Object>> rows = new ArrayList<>();
            for (Segment segment : snapshot.get().getSegments()) {
                WriterFileSet wfs = segment.dfGroupedSearchableFiles().get("parquet");
                if (wfs == null) {
                    continue;
                }
                for (String file : wfs.files()) {
                    rows.addAll(parseJsonRows(RustBridge.readAsJson(parquetDir.resolve(file).toString())));
                }
            }
            return rows;
        }
    }

    @SuppressWarnings("unchecked")
    @SuppressForbidden(reason = "JSON parsing for test verification of parquet output")
    private static List<Map<String, Object>> parseJsonRows(String json) throws IOException {
        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                json
            )
        ) {
            return parser.list().stream().map(o -> (Map<String, Object>) o).collect(Collectors.toList());
        }
    }
}
