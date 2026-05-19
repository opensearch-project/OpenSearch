/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.engine;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.opensearch.Version;
import org.opensearch.analytics.schema.OpenSearchSchemaBuilder;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.test.OpenSearchTestCase;

import java.util.LinkedHashMap;
import java.util.Map;

public class OpenSearchSchemaBuilderTests extends OpenSearchTestCase {

    /**
     * Test that buildSchema produces a table for each index with correct column types.
     * Type mapping: keyword->VARCHAR, long->BIGINT, double->DOUBLE
     */
    public void testBuildSchemaWithKeywordLongDouble() throws Exception {
        ClusterState clusterState = buildClusterState(Map.of("test_index", Map.of("name", "keyword", "age", "long", "score", "double")));

        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterState);

        Table table = schema.getTable("test_index");
        assertNotNull("Table test_index should exist in schema", table);

        RelDataType rowType = table.getRowType(new org.apache.calcite.jdbc.JavaTypeFactoryImpl());
        assertEquals(3, rowType.getFieldCount());

        assertFieldType(rowType, "name", SqlTypeName.VARCHAR);
        assertFieldType(rowType, "age", SqlTypeName.BIGINT);
        assertFieldType(rowType, "score", SqlTypeName.DOUBLE);
    }

    /**
     * Test integer, float, boolean type mappings.
     */
    public void testBuildSchemaWithIntegerFloatBoolean() throws Exception {
        ClusterState clusterState = buildClusterState(
            Map.of("types_index", Map.of("count", "integer", "ratio", "float", "active", "boolean"))
        );

        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterState);

        Table table = schema.getTable("types_index");
        assertNotNull("Table types_index should exist in schema", table);

        RelDataType rowType = table.getRowType(new org.apache.calcite.jdbc.JavaTypeFactoryImpl());
        assertFieldType(rowType, "count", SqlTypeName.INTEGER);
        assertFieldType(rowType, "ratio", SqlTypeName.REAL);
        assertFieldType(rowType, "active", SqlTypeName.BOOLEAN);
    }

    /**
     * Test date, ip, text, short, byte type mappings.
     */
    public void testBuildSchemaWithDateIpTextShortByte() throws Exception {
        ClusterState clusterState = buildClusterState(
            Map.of("more_types", Map.of("created", "date", "address", "ip", "content", "text", "small_num", "short", "tiny_num", "byte"))
        );

        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterState);

        Table table = schema.getTable("more_types");
        assertNotNull("Table more_types should exist in schema", table);

        RelDataType rowType = table.getRowType(new org.apache.calcite.jdbc.JavaTypeFactoryImpl());
        assertFieldType(rowType, "created", SqlTypeName.TIMESTAMP);
        assertFieldType(rowType, "address", SqlTypeName.VARBINARY);
        assertFieldType(rowType, "content", SqlTypeName.VARCHAR);
        assertFieldType(rowType, "small_num", SqlTypeName.SMALLINT);
        assertFieldType(rowType, "tiny_num", SqlTypeName.TINYINT);
    }

    /**
     * Test that multiple indices produce multiple tables.
     */
    public void testMultipleIndicesProduceMultipleTables() throws Exception {
        IndexMetadata idx1 = buildIndexMetadata("index_a", Map.of("col1", "keyword"));
        IndexMetadata idx2 = buildIndexMetadata("index_b", Map.of("col2", "long"));

        Metadata metadata = Metadata.builder().put(idx1, false).put(idx2, false).build();

        ClusterState clusterState = ClusterState.builder(new ClusterName("test")).metadata(metadata).build();

        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterState);

        assertNotNull("Table index_a should exist", schema.getTable("index_a"));
        assertNotNull("Table index_b should exist", schema.getTable("index_b"));
    }

    /**
     * Test that nested/object fields are skipped.
     */
    public void testNestedAndObjectFieldsSkipped() throws Exception {
        ClusterState clusterState = buildClusterState(
            Map.of("nested_index", Map.of("name", "keyword", "address", "object", "tags", "nested"))
        );

        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterState);

        Table table = schema.getTable("nested_index");
        assertNotNull(table);

        RelDataType rowType = table.getRowType(new org.apache.calcite.jdbc.JavaTypeFactoryImpl());
        assertEquals("Should only have 'name' field, skipping object/nested", 1, rowType.getFieldCount());
        assertFieldType(rowType, "name", SqlTypeName.VARCHAR);
    }

    /**
     * Test that an empty ClusterState produces an empty schema.
     */
    public void testEmptyClusterStateProducesEmptySchema() {
        ClusterState clusterState = ClusterState.builder(new ClusterName("test")).metadata(Metadata.builder().build()).build();

        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterState);
        assertNotNull(schema);
        assertTrue("Schema should have no tables", schema.getTableNames().isEmpty());
    }

    /**
     * Test mapFieldType for all supported types.
     */
    public void testMapFieldTypeForAllSupportedTypes() {
        assertEquals(SqlTypeName.VARCHAR, OpenSearchSchemaBuilder.mapFieldType("keyword"));
        assertEquals(SqlTypeName.VARCHAR, OpenSearchSchemaBuilder.mapFieldType("text"));
        assertEquals(SqlTypeName.VARCHAR, OpenSearchSchemaBuilder.mapFieldType("match_only_text"));
        assertEquals(SqlTypeName.BIGINT, OpenSearchSchemaBuilder.mapFieldType("long"));
        assertEquals(SqlTypeName.BIGINT, OpenSearchSchemaBuilder.mapFieldType("unsigned_long"));
        assertEquals(SqlTypeName.BIGINT, OpenSearchSchemaBuilder.mapFieldType("scaled_float"));
        assertEquals(SqlTypeName.INTEGER, OpenSearchSchemaBuilder.mapFieldType("integer"));
        assertEquals(SqlTypeName.SMALLINT, OpenSearchSchemaBuilder.mapFieldType("short"));
        assertEquals(SqlTypeName.TINYINT, OpenSearchSchemaBuilder.mapFieldType("byte"));
        assertEquals(SqlTypeName.DOUBLE, OpenSearchSchemaBuilder.mapFieldType("double"));
        assertEquals(SqlTypeName.REAL, OpenSearchSchemaBuilder.mapFieldType("float"));
        assertEquals(SqlTypeName.REAL, OpenSearchSchemaBuilder.mapFieldType("half_float"));
        assertEquals(SqlTypeName.BOOLEAN, OpenSearchSchemaBuilder.mapFieldType("boolean"));
        assertEquals(SqlTypeName.TIMESTAMP, OpenSearchSchemaBuilder.mapFieldType("date"));
        assertEquals(SqlTypeName.TIMESTAMP, OpenSearchSchemaBuilder.mapFieldType("date_nanos"));
        assertEquals(SqlTypeName.VARBINARY, OpenSearchSchemaBuilder.mapFieldType("ip"));
        assertEquals(SqlTypeName.VARBINARY, OpenSearchSchemaBuilder.mapFieldType("binary"));
    }

    /**
     * Unsupported / unknown field types degrade to a null SqlTypeName. The schema builder drops
     * such columns so a downstream query referencing one fails at validation time with
     * "column not found", not at planning time with an IllegalArgumentException.
     */
    public void testUnsupportedFieldTypesReturnNull() {
        for (String unsupported : new String[] {
            "geo_point",
            "geo_shape",
            "point",
            "shape",
            "completion",
            "constant_keyword",
            "wildcard",
            "alias",
            "flat_object",
            "dense_vector",
            "sparse_vector",
            "percolator",
            "integer_range",
            "long_range",
            "date_range",
            "ip_range",
            "token_count",
            "version",
            "made_up_plugin_type" }) {
            assertNull("Expected null for unsupported type [" + unsupported + "]", OpenSearchSchemaBuilder.mapFieldType(unsupported));
        }
    }

    /**
     * Indices that mix supported and unsupported fields produce a schema containing only the
     * supported ones — the unsupported columns are dropped silently rather than aborting the
     * entire schema build for the index.
     */
    public void testIndexWithUnsupportedFieldsDropsThoseColumns() throws Exception {
        ClusterState clusterState = buildClusterState(
            Map.of("mixed_index", Map.of("name", "keyword", "location", "geo_point", "shape_field", "geo_shape", "age", "long"))
        );

        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterState);
        Table table = schema.getTable("mixed_index");
        assertNotNull("Table mixed_index should exist despite unsupported fields", table);

        RelDataType rowType = table.getRowType(new org.apache.calcite.jdbc.JavaTypeFactoryImpl());
        assertEquals("Only the 2 supported fields should be present", 2, rowType.getFieldCount());
        assertFieldType(rowType, "name", SqlTypeName.VARCHAR);
        assertFieldType(rowType, "age", SqlTypeName.BIGINT);
        assertNull("geo_point column must be dropped", rowType.getField("location", true, false));
        assertNull("geo_shape column must be dropped", rowType.getField("shape_field", true, false));
    }

    /**
     * Index whose every field is unsupported still emits a Calcite Table — with an empty rowType.
     * The build itself must not throw; downstream validation rejects any column reference
     * naturally because the rowType has zero columns.
     */
    public void testAllUnsupportedFieldsProduceEmptyRowType() throws Exception {
        ClusterState clusterState = buildClusterState(
            Map.of("all_unsupported", Map.of("loc", "geo_point", "shape", "geo_shape", "feat", "completion"))
        );

        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterState);
        Table table = schema.getTable("all_unsupported");
        assertNotNull("Table must exist even when every field is unsupported", table);

        RelDataType rowType = table.getRowType(new org.apache.calcite.jdbc.JavaTypeFactoryImpl());
        assertEquals("All-unsupported index produces 0-column rowType", 0, rowType.getFieldCount());
    }

    /**
     * Nested object containing geo_point + supported leaves. Confirms the recursive path in
     * addLeafFields drops the unsupported leaf without aborting the recursion or losing
     * sibling supported columns.
     */
    public void testNestedObjectWithUnsupportedLeafDropsOnlyThatLeaf() throws Exception {
        String mapping = "{\"properties\":{"
            + "\"customer\":{\"properties\":{"
            + "\"id\":{\"type\":\"keyword\"},"
            + "\"home\":{\"type\":\"geo_point\"},"
            + "\"age\":{\"type\":\"integer\"}"
            + "}}"
            + "}}";
        ClusterState clusterState = buildClusterStateRaw("nested_geo", mapping);

        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterState);
        Table table = schema.getTable("nested_geo");
        assertNotNull(table);

        RelDataType rowType = table.getRowType(new org.apache.calcite.jdbc.JavaTypeFactoryImpl());
        assertEquals("Only 2 supported nested leaves should remain", 2, rowType.getFieldCount());
        assertFieldType(rowType, "customer.id", SqlTypeName.VARCHAR);
        assertFieldType(rowType, "customer.age", SqlTypeName.INTEGER);
        assertNull("nested geo_point leaf must be dropped", rowType.getField("customer.home", true, false));
    }

    /**
     * Calcite end-to-end: parsing and validating a query that projects a SUPPORTED column on an
     * index whose mapping contains an unsupported field must succeed. This is the scan_viability
     * bug reproducer pattern — geo_point in mapping but not in projection.
     */
    public void testCalciteValidatesQuerySkippingUnsupportedField() throws Exception {
        ClusterState clusterState = buildClusterState(Map.of("kai_repro", Map.of("name", "keyword", "loc", "geo_point", "age", "long")));
        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterState);

        RelNode rel = parseValidateConvert(schema, "SELECT name, age FROM kai_repro");
        assertNotNull("Calcite should produce a RelNode for projection-of-supported-only", rel);
        assertEquals("Result must have exactly the 2 projected columns", 2, rel.getRowType().getFieldCount());
        // Calcite uppercases unquoted identifiers by default; compare case-insensitively.
        assertEquals("name", rel.getRowType().getFieldList().get(0).getName().toLowerCase(java.util.Locale.ROOT));
        assertEquals("age", rel.getRowType().getFieldList().get(1).getName().toLowerCase(java.util.Locale.ROOT));
    }

    /**
     * Calcite end-to-end: projecting a DROPPED unsupported column must surface as a validator
     * error naming the column — not as IllegalArgumentException, NPE, or silent null.
     * This is the load-bearing contract behind the schema-degradation fix.
     */
    public void testCalciteRejectsQueryReferencingDroppedColumn() throws Exception {
        ClusterState clusterState = buildClusterState(Map.of("kai_repro", Map.of("name", "keyword", "loc", "geo_point")));
        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterState);

        Exception ex = expectThrows(Exception.class, () -> parseValidateConvert(schema, "SELECT loc FROM kai_repro"));
        // Calcite wraps validator errors; walk the cause chain for the column name.
        String fullMessage = collectMessages(ex);
        assertTrue(
            "Validator error must mention dropped column 'loc'; got: " + fullMessage,
            fullMessage.toLowerCase(java.util.Locale.ROOT).contains("loc")
        );
        assertFalse(
            "Must NOT surface as IllegalArgumentException 'Unsupported OpenSearch field type'; got: " + fullMessage,
            fullMessage.contains("Unsupported OpenSearch field type")
        );
    }

    /**
     * Field with no "type" property and no "properties" sub-map (malformed mapping). The
     * existing object-recursion branch enters but finds nothing to add, so the field is
     * silently skipped — must not throw.
     */
    public void testMalformedFieldWithoutTypeOrPropertiesIsSkipped() throws Exception {
        String mapping = "{\"properties\":{"
            + "\"normal\":{\"type\":\"keyword\"},"
            + "\"weird\":{\"index\":\"not_analyzed\"}"  // no "type", no "properties"
            + "}}";
        ClusterState clusterState = buildClusterStateRaw("malformed_idx", mapping);

        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterState);
        Table table = schema.getTable("malformed_idx");
        assertNotNull(table);

        RelDataType rowType = table.getRowType(new org.apache.calcite.jdbc.JavaTypeFactoryImpl());
        assertEquals("Only the well-formed field should survive", 1, rowType.getFieldCount());
        assertFieldType(rowType, "normal", SqlTypeName.VARCHAR);
    }

    /**
     * Multi-fields (`fields`) sub-mapping is currently NOT walked — only the top-level
     * "type" matters. An unsupported multi-field subtype thus has zero effect on the parent
     * column. Pinning current behavior so a future change to walk `fields` is intentional.
     */
    public void testMultiFieldsSubtypeDoesNotAffectParent() throws Exception {
        String mapping = "{\"properties\":{"
            + "\"name\":{\"type\":\"keyword\",\"fields\":{"
            + "\"raw\":{\"type\":\"keyword\"},"
            + "\"loc\":{\"type\":\"geo_point\"}"
            + "}}"
            + "}}";
        ClusterState clusterState = buildClusterStateRaw("multifield_idx", mapping);

        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterState);
        Table table = schema.getTable("multifield_idx");
        assertNotNull(table);

        RelDataType rowType = table.getRowType(new org.apache.calcite.jdbc.JavaTypeFactoryImpl());
        assertEquals("Parent field is preserved; multi-fields subtree is ignored", 1, rowType.getFieldCount());
        assertFieldType(rowType, "name", SqlTypeName.VARCHAR);
        assertNull("Multi-field 'raw' is not currently surfaced as a column", rowType.getField("name.raw", true, false));
        assertNull("Multi-field 'loc' is not currently surfaced as a column", rowType.getField("name.loc", true, false));
    }

    /**
     * Defensive: mapFieldType called with null must return null (treated as "unknown") rather
     * than NPE. Mirrors the analytics-framework FieldType.fromMappingType convention.
     */
    public void testMapFieldTypeReturnsNullOnNullInput() {
        assertNull(OpenSearchSchemaBuilder.mapFieldType(null));
    }

    // --- helpers ---

    private void assertFieldType(RelDataType rowType, String fieldName, SqlTypeName expectedType) {
        RelDataTypeField field = rowType.getField(fieldName, true, false);
        assertNotNull("Field '" + fieldName + "' should exist", field);
        assertEquals("Field '" + fieldName + "' should have type " + expectedType, expectedType, field.getType().getSqlTypeName());
    }

    private ClusterState buildClusterState(Map<String, Map<String, String>> indices) throws Exception {
        Metadata.Builder metadataBuilder = Metadata.builder();
        for (Map.Entry<String, Map<String, String>> entry : indices.entrySet()) {
            metadataBuilder.put(buildIndexMetadata(entry.getKey(), entry.getValue()), false);
        }
        return ClusterState.builder(new ClusterName("test")).metadata(metadataBuilder.build()).build();
    }

    private ClusterState buildClusterStateRaw(String indexName, String mappingJson) throws Exception {
        IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putMapping(mappingJson)
            .build();
        Metadata metadata = Metadata.builder().put(indexMetadata, false).build();
        return ClusterState.builder(new ClusterName("test")).metadata(metadata).build();
    }

    private IndexMetadata buildIndexMetadata(String indexName, Map<String, String> fieldTypes) throws Exception {
        // LinkedHashMap preserves declaration order so the assertions on field positions are stable.
        Map<String, String> ordered = new LinkedHashMap<>(fieldTypes);
        StringBuilder mappingJson = new StringBuilder("{\"properties\":{");
        boolean first = true;
        for (Map.Entry<String, String> field : ordered.entrySet()) {
            if (!first) mappingJson.append(",");
            mappingJson.append("\"").append(field.getKey()).append("\":{\"type\":\"").append(field.getValue()).append("\"}");
            first = false;
        }
        mappingJson.append("}}");

        return IndexMetadata.builder(indexName)
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putMapping(mappingJson.toString())
            .build();
    }

    /** Parse + validate + convert SQL against the given Calcite schema; throws on validator error. */
    private static RelNode parseValidateConvert(SchemaPlus schema, String sql) throws Exception {
        FrameworkConfig config = Frameworks.newConfigBuilder()
            .parserConfig(SqlParser.config().withCaseSensitive(false))
            .defaultSchema(schema)
            .operatorTable(SqlStdOperatorTable.instance())
            .build();
        Planner planner = Frameworks.getPlanner(config);
        try {
            SqlNode parsed = planner.parse(sql);
            SqlNode validated = planner.validate(parsed);
            return planner.rel(validated).project();
        } finally {
            planner.close();
        }
    }

    /** Concatenate the message of the throwable and every cause for substring assertions. */
    private static String collectMessages(Throwable t) {
        StringBuilder sb = new StringBuilder();
        for (Throwable cur = t; cur != null; cur = cur.getCause()) {
            if (cur.getMessage() != null) {
                sb.append(cur.getMessage()).append(" | ");
            }
        }
        return sb.toString();
    }
}
