/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.Version;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.analytics.AnalyticsPlugin;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.composite.CompositeDataFormatPlugin;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.ppl.TestPPLPlugin;
import org.opensearch.ppl.action.PPLRequest;
import org.opensearch.ppl.action.PPLResponse;
import org.opensearch.ppl.action.UnifiedPPLExecuteAction;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Shared fixture + scalar-result assert helpers for end-to-end PPL → Calcite →
 * Substrait → DataFusion scalar-function tests.
 *
 * <p>Each subclass declares its functions as test methods using the
 * {@code assertScalarXxx(expr, expected)} helpers. The query template is fixed:
 * {@code source=bank | eval x = <expr> | fields x | head 1}. Inputs are
 * literals so assertions don't depend on the bank fixture's data — the test
 * exercises the function's name lookup, type inference, and runtime, not
 * arithmetic on rows.
 *
 * @opensearch.internal
 */
// TEST-scope cluster per method — slower but eliminates cluster-reuse degradation that
// surfaces as cascading NodeDisconnectedException when many test methods share a SUITE cluster.
// supportsDedicatedMasters=false + numClientNodes=0 collapses the cluster to a single node
// combining cluster-manager and data roles: scalar-function tests exercise query rewrite +
// single-shard execution, which doesn't need dedicated cluster-managers or a separate
// coord-only node. The 5-node default (3 cluster-managers + 1 data + 1 coord) is a memory
// pressure source that destabilises node discovery on resource-constrained runners.
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1, supportsDedicatedMasters = false, numClientNodes = 0)
public abstract class BaseScalarFunctionIT extends OpenSearchIntegTestCase {

    protected static final String BANK_INDEX = "bank";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TestPPLPlugin.class, FlightStreamPlugin.class, CompositeDataFormatPlugin.class, LucenePlugin.class);
    }

    @Override
    protected Collection<PluginInfo> additionalNodePlugins() {
        return List.of(
            classpathPlugin(AnalyticsPlugin.class, Collections.emptyList()),
            classpathPlugin(ParquetDataFormatPlugin.class, Collections.emptyList()),
            classpathPlugin(DataFusionPlugin.class, List.of(AnalyticsPlugin.class.getName()))
        );
    }

    private static PluginInfo classpathPlugin(Class<? extends Plugin> pluginClass, List<String> extendedPlugins) {
        return new PluginInfo(
            pluginClass.getName(),
            "classpath plugin",
            "NA",
            Version.CURRENT,
            "1.8",
            pluginClass.getName(),
            null,
            extendedPlugins,
            false
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            .build();
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // SUITE-scoped cluster is reused across test methods — only create/index once.
        if (!indexExists(BANK_INDEX)) {
            createBankIndex();
            indexBankDocs();
            ensureGreen(BANK_INDEX);
            refresh(BANK_INDEX);
        }
    }

    private void createBankIndex() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("account_number")
            .field("type", "long")
            .endObject()
            .startObject("firstname")
            .field("type", "keyword")
            .endObject()
            .startObject("balance")
            .field("type", "long")
            .endObject()
            .startObject("created_at")
            .field("type", "date")
            .endObject()
            // json_str holds serialized JSON arrays/objects/malformed strings so
            // scalar-JSON UDFs can be exercised on real column values (columnar
            // UDF path), not just string literals (scalar fast-path).
            .startObject("json_str")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject();

        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats")
            .build();

        CreateIndexResponse response = client().admin()
            .indices()
            .prepareCreate(BANK_INDEX)
            .setSettings(indexSettings)
            .setMapping(mapping)
            .get();
        assertTrue("bank index creation must be acknowledged", response.isAcknowledged());
    }

    private void indexBankDocs() {
        // Row 1 carries a 3-element JSON array in json_str; row 6 carries a JSON object.
        // This lets scalar-JSON UDF tests assert both the happy path (row 1 → length 3)
        // and the non-array → NULL path (row 6) from real column values.
        client().prepareIndex(BANK_INDEX)
            .setId("1")
            .setSource(
                "account_number",
                1,
                "firstname",
                "Amber",
                "balance",
                39225L,
                "created_at",
                "2024-06-15T10:30:00Z",
                "json_str",
                "[1,2,3]"
            )
            .get();
        client().prepareIndex(BANK_INDEX)
            .setId("6")
            .setSource(
                "account_number",
                6,
                "firstname",
                "Hattie",
                "balance",
                5686L,
                "created_at",
                "2024-01-20T14:45:30Z",
                "json_str",
                "{\"k\":1}"
            )
            .get();
    }

    // ---- Assert helpers ----

    /**
     * Runs the given expression against the single bank row with
     * {@code account_number=1} (firstname='Amber', balance=39225) and returns
     * the resulting cell. Pinning the row makes assertions deterministic and
     * lets tests reference {@code firstname} / {@code balance} as fields —
     * which prevents Calcite's constant-folding from optimizing the function
     * away at plan time. Tests must therefore use field references to truly
     * exercise the Substrait + DataFusion runtime path.
     */
    protected Object evalScalar(String expr) {
        PPLRequest request = new PPLRequest(
            "source=" + BANK_INDEX + " | where account_number = 1 | eval x = " + expr + " | fields x | head 1"
        );
        PPLResponse response = client().execute(UnifiedPPLExecuteAction.INSTANCE, request).actionGet();
        assertNotNull("PPLResponse must not be null", response);
        assertEquals("schema columns", List.of("x"), response.getColumns());
        assertEquals("head 1 → exactly 1 row", 1, response.getRows().size());
        return response.getRows().get(0)[0];
    }

    protected void assertScalarLong(String expr, long expected) {
        Object cell = evalScalar(expr);
        assertNotNull(expr + " result must not be null", cell);
        assertTrue(expr + " result must be Number, got " + cell.getClass(), cell instanceof Number);
        assertEquals(expr, expected, ((Number) cell).longValue());
    }

    /**
     * Strict variant that asserts the cell is a {@link Long} (not just a {@link Number}).
     * Use for functions whose on-wire BIGINT return type must not silently regress.
     */
    protected void assertScalarLongStrict(String expr, long expected) {
        Object cell = evalScalar(expr);
        assertNotNull(expr + " result must not be null", cell);
        assertTrue(expr + " result must be Long, got " + cell.getClass(), cell instanceof Long);
        assertEquals(expr, expected, ((Long) cell).longValue());
    }

    /**
     * Strict variant that asserts the cell is an {@link Integer}. Use for functions
     * whose on-wire INTEGER return type must be preserved through the pipeline —
     * e.g. PPL scalar UDFs declared as {@code INTEGER_FORCE_NULLABLE} whose Rust
     * implementations return {@code Int64} but get narrowed via an implicit CAST
     * on the enclosing Project. The non-strict {@link #assertScalarLong} silently
     * accepts either width and would miss this contract regression.
     */
    protected void assertScalarIntStrict(String expr, int expected) {
        Object cell = evalScalar(expr);
        assertNotNull(expr + " result must not be null", cell);
        assertTrue(expr + " result must be Integer, got " + cell.getClass(), cell instanceof Integer);
        assertEquals(expr, expected, ((Integer) cell).intValue());
    }

    protected void assertScalarDouble(String expr, double expected, double delta) {
        Object cell = evalScalar(expr);
        assertNotNull(expr + " result must not be null", cell);
        assertTrue(expr + " result must be Number, got " + cell.getClass(), cell instanceof Number);
        assertEquals(expr, expected, ((Number) cell).doubleValue(), delta);
    }

    protected void assertScalarString(String expr, String expected) {
        Object cell = evalScalar(expr);
        assertNotNull(expr + " result must not be null", cell);
        assertEquals(expr, expected, cell.toString());
    }

    protected void assertScalarBoolean(String expr, boolean expected) {
        Object cell = evalScalar(expr);
        assertNotNull(expr + " result must not be null", cell);
        assertTrue(expr + " result must be Boolean, got " + cell.getClass(), cell instanceof Boolean);
        assertEquals(expr, expected, cell);
    }

    protected void assertScalarNull(String expr) {
        Object cell = evalScalar(expr);
        assertNull(expr + " result must be null but was " + cell, cell);
    }
}
