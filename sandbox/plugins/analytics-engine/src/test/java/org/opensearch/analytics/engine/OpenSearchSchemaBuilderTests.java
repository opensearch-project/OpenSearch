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
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.analytics.schema.BinaryType;
import org.opensearch.analytics.schema.DateOnlyType;
import org.opensearch.analytics.schema.IpType;
import org.opensearch.analytics.schema.OpenSearchSchemaBuilder;
import org.opensearch.analytics.schema.ScaledFloatType;
import org.opensearch.analytics.schema.TimeOnlyType;
import org.opensearch.analytics.schema.UnsignedLongType;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.AliasMetadata;
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
     * Case-insensitive table resolution: Calcite uppercases unquoted identifiers. The schema
     * must still resolve them. Verifying a SELECT against an uppercased table works.
     */
    /**
     * The schema's lazy {@code get()} lower-cases incoming lookup names before consulting
     * {@code IndexNameExpressionResolver} (OpenSearch index names must be lowercase). This lets
     * Calcite's uppercased identifier resolution find a lower-cased index without needing eager
     * enumeration of the cluster's index list at schema construction.
     */
    public void testCaseInsensitiveTableResolution() throws Exception {
        ClusterState clusterState = buildClusterState(Map.of("my_table", Map.of("name", "keyword", "age", "long")));
        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterState);

        // Exact-case lookup must work — this is the storage truth.
        assertNotNull("Exact-case schema lookup must work", schema.getTable("my_table"));

        // Upper-cased lookup must work — exercises the lowercase-on-lookup behavior directly
        // (bypasses the SQL parser, which would case-fold first). Pins the regression Calcite
        // would trigger if the schema regressed to case-sensitive resolver invocation.
        assertNotNull("Upper-case schema lookup must work", schema.getTable("MY_TABLE"));
        assertNotNull("Mixed-case schema lookup must work", schema.getTable("My_Table"));

        // End-to-end via the SQL path: validate+convert succeeds against the lower-cased index.
        RelNode rel = parseValidateConvert(schema, "SELECT name FROM my_table");
        assertNotNull("Query via SQL path must succeed", rel);
    }

    /**
     * Defensive: mapFieldType called with null must return null (treated as "unknown") rather
     * than NPE. Mirrors the analytics-framework FieldType.fromMappingType convention.
     */
    public void testMapFieldTypeReturnsNullOnNullInput() {
        assertNull(OpenSearchSchemaBuilder.mapFieldType(null));
    }

    /** date with date-only format produces a DateOnlyType marker (TIMESTAMP-backed). */
    public void testDateFieldWithDateOnlyFormatProducesDateUDT() throws Exception {
        String mapping = "{\"properties\":{\"d\":{\"type\":\"date\",\"format\":\"basic_date\"}}}";
        ClusterState clusterState = buildClusterStateRaw("date_only_idx", mapping);
        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterState);
        Table table = schema.getTable("date_only_idx");
        RelDataType rowType = table.getRowType(new org.apache.calcite.jdbc.JavaTypeFactoryImpl());
        RelDataTypeField field = rowType.getField("d", true, false);
        assertNotNull(field);
        assertTrue("Expected DateOnlyType marker, got " + field.getType().getClass(), field.getType() instanceof DateOnlyType);
        assertEquals(SqlTypeName.TIMESTAMP, field.getType().getSqlTypeName());
    }

    /** date with time-only format produces a TimeOnlyType marker. */
    public void testDateFieldWithTimeOnlyFormatProducesTimeUDT() throws Exception {
        String mapping = "{\"properties\":{\"t\":{\"type\":\"date\",\"format\":\"hour_minute_second\"}}}";
        ClusterState clusterState = buildClusterStateRaw("time_only_idx", mapping);
        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterState);
        Table table = schema.getTable("time_only_idx");
        RelDataType rowType = table.getRowType(new org.apache.calcite.jdbc.JavaTypeFactoryImpl());
        RelDataTypeField field = rowType.getField("t", true, false);
        assertNotNull(field);
        assertTrue("Expected TimeOnlyType marker, got " + field.getType().getClass(), field.getType() instanceof TimeOnlyType);
        assertEquals(SqlTypeName.TIMESTAMP, field.getType().getSqlTypeName());
    }

    /** date with mixed format defaults to plain TIMESTAMP. */
    public void testDateFieldWithMixedFormatStaysTimestamp() throws Exception {
        String mapping = "{\"properties\":{\"x\":{\"type\":\"date\",\"format\":\"yyyy-MM-dd HH:mm:ss\"}}}";
        ClusterState clusterState = buildClusterStateRaw("mixed_idx", mapping);
        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterState);
        Table table = schema.getTable("mixed_idx");
        RelDataType rowType = table.getRowType(new org.apache.calcite.jdbc.JavaTypeFactoryImpl());
        RelDataTypeField field = rowType.getField("x", true, false);
        assertNotNull(field);
        assertFalse("mixed format must NOT produce a Date/Time UDT", field.getType() instanceof DateOnlyType);
        assertFalse("mixed format must NOT produce a Date/Time UDT", field.getType() instanceof TimeOnlyType);
        assertEquals(SqlTypeName.TIMESTAMP, field.getType().getSqlTypeName());
    }

    /** date without an explicit format keeps default TIMESTAMP behavior. */
    public void testDateFieldWithoutFormatStaysTimestamp() throws Exception {
        ClusterState clusterState = buildClusterState(java.util.Map.of("plain_date_idx", java.util.Map.of("d", "date")));
        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterState);
        Table table = schema.getTable("plain_date_idx");
        RelDataType rowType = table.getRowType(new org.apache.calcite.jdbc.JavaTypeFactoryImpl());
        RelDataTypeField field = rowType.getField("d", true, false);
        assertFalse(field.getType() instanceof DateOnlyType);
        assertFalse(field.getType() instanceof TimeOnlyType);
        assertEquals(SqlTypeName.TIMESTAMP, field.getType().getSqlTypeName());
    }

    /**
     * Plain {@code date} with no format → {@code TIMESTAMP(3)} (millis). Pins the precision
     * contract added by #22049 so the date-only / time-only UDT paths can mirror it.
     */
    public void testDateFieldWithoutFormatHasMillisPrecision() throws Exception {
        ClusterState clusterState = buildClusterState(java.util.Map.of("plain_date_idx", java.util.Map.of("d", "date")));
        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterState);
        RelDataTypeField field = schema.getTable("plain_date_idx").getRowType(nanosTypeFactory()).getField("d", true, false);
        assertEquals("date must carry millis precision", 3, field.getType().getPrecision());
    }

    /** Plain {@code date_nanos} with no format → {@code TIMESTAMP(9)} (nanos). */
    public void testDateNanosFieldWithoutFormatHasNanosPrecision() throws Exception {
        ClusterState clusterState = buildClusterState(java.util.Map.of("nanos_idx", java.util.Map.of("d", "date_nanos")));
        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterState);
        RelDataTypeField field = schema.getTable("nanos_idx").getRowType(nanosTypeFactory()).getField("d", true, false);
        assertEquals("date_nanos must carry nanos precision", 9, field.getType().getPrecision());
    }

    /**
     * {@code date} with date-only format produces a {@link DateOnlyType} that still carries
     * millis precision. Without this, the parquet-read path's Timestamp(Millisecond) clashes
     * with the schema's precision-0 default at coordinator reduce.
     */
    public void testDateFieldWithDateOnlyFormatHasMillisPrecision() throws Exception {
        String mapping = "{\"properties\":{\"d\":{\"type\":\"date\",\"format\":\"basic_date\"}}}";
        ClusterState clusterState = buildClusterStateRaw("date_only_ms_idx", mapping);
        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterState);
        RelDataTypeField field = schema.getTable("date_only_ms_idx").getRowType(nanosTypeFactory()).getField("d", true, false);
        assertTrue("Expected DateOnlyType marker", field.getType() instanceof DateOnlyType);
        assertEquals("DateOnlyType over date must carry millis precision", 3, field.getType().getPrecision());
    }

    /**
     * {@code date_nanos} with date-only format produces a {@link DateOnlyType} that carries
     * nanos precision. This is the regression #22049 fixed for the plain-TIMESTAMP path; the
     * UDT path must mirror it or multi-shard sort fails with
     * {@code Timestamp(ms) got Timestamp(ns)} in the coordinator RowConverter.
     */
    public void testDateNanosFieldWithDateOnlyFormatHasNanosPrecision() throws Exception {
        String mapping = "{\"properties\":{\"d\":{\"type\":\"date_nanos\",\"format\":\"basic_date\"}}}";
        ClusterState clusterState = buildClusterStateRaw("date_only_ns_idx", mapping);
        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterState);
        RelDataTypeField field = schema.getTable("date_only_ns_idx").getRowType(nanosTypeFactory()).getField("d", true, false);
        assertTrue("Expected DateOnlyType marker", field.getType() instanceof DateOnlyType);
        assertEquals("DateOnlyType over date_nanos must carry nanos precision", 9, field.getType().getPrecision());
    }

    /**
     * {@code date_nanos} with time-only format produces a {@link TimeOnlyType} that carries
     * nanos precision — same parquet-read constraint as {@link DateOnlyType}.
     */
    public void testDateNanosFieldWithTimeOnlyFormatHasNanosPrecision() throws Exception {
        String mapping = "{\"properties\":{\"t\":{\"type\":\"date_nanos\",\"format\":\"hour_minute_second\"}}}";
        ClusterState clusterState = buildClusterStateRaw("time_only_ns_idx", mapping);
        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterState);
        RelDataTypeField field = schema.getTable("time_only_ns_idx").getRowType(nanosTypeFactory()).getField("t", true, false);
        assertTrue("Expected TimeOnlyType marker", field.getType() instanceof TimeOnlyType);
        assertEquals("TimeOnlyType over date_nanos must carry nanos precision", 9, field.getType().getPrecision());
    }

    /**
     * Type factory whose {@code getMaxPrecision(TIMESTAMP)} is 9 — so a precision request from
     * {@code buildLeafType} for {@code date_nanos} is observable rather than silently clamped to
     * Calcite's default of 3. The production type factory mirrors this; the default
     * {@code JavaTypeFactoryImpl} doesn't.
     */
    private static org.apache.calcite.jdbc.JavaTypeFactoryImpl nanosTypeFactory() {
        return new org.apache.calcite.jdbc.JavaTypeFactoryImpl(new org.apache.calcite.rel.type.RelDataTypeSystemImpl() {
            @Override
            public int getMaxPrecision(SqlTypeName typeName) {
                if (typeName == SqlTypeName.TIMESTAMP || typeName == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
                    return 9;
                }
                return super.getMaxPrecision(typeName);
            }
        });
    }

    /**
     * IP and binary fields are wrapped in dedicated {@link IpType} / {@link BinaryType} markers
     * so the SQL plugin can disambiguate them at the response boundary. Operator dispatch is
     * unaffected because both extend {@link org.apache.calcite.sql.type.AbstractSqlType} with
     * {@link SqlTypeName#VARBINARY} underneath.
     */
    public void testIpAndBinaryFieldsCarryLogicalTypeUdt() throws Exception {
        ClusterState clusterState = buildClusterState(Map.of("udt_index", Map.of("address", "ip", "blob", "binary")));

        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterState);
        Table table = schema.getTable("udt_index");
        assertNotNull(table);

        RelDataType rowType = table.getRowType(new org.apache.calcite.jdbc.JavaTypeFactoryImpl());

        RelDataType ipType = rowType.getField("address", true, false).getType();
        assertTrue("ip column must be IpType, got " + ipType.getClass(), ipType instanceof IpType);
        assertEquals("IpType must still report VARBINARY for operator dispatch", SqlTypeName.VARBINARY, ipType.getSqlTypeName());

        RelDataType binType = rowType.getField("blob", true, false).getType();
        assertTrue("binary column must be BinaryType, got " + binType.getClass(), binType instanceof BinaryType);
        assertEquals(SqlTypeName.VARBINARY, binType.getSqlTypeName());
    }

    /**
     * Two separately constructed {@link IpType} instances must be digest-equal so plan equality
     * / planner caching works without requiring Calcite type-factory canonicalization (we don't
     * go through {@code typeFactory.canonize}).
     */
    public void testLogicalTypeEqualityIsDigestBased() {
        RelDataType a = IpType.nullable();
        RelDataType b = IpType.nullable();
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());

        RelDataType c = BinaryType.nullable();
        assertNotSame(a, c);
        assertNotEquals(a, c);
    }

    // --- scaled_float tests ---

    /** scaled_float with valid factor produces a ScaledFloatType marker (BIGINT-backed). */
    public void testScaledFloatFieldWithFactorProducesScaledFloatType() throws Exception {
        String mapping = "{\"properties\":{\"price\":{\"type\":\"scaled_float\",\"scaling_factor\":10}}}";
        ClusterState clusterState = buildClusterStateRaw("sf_idx", mapping);
        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterState);
        Table table = schema.getTable("sf_idx");
        RelDataType rowType = table.getRowType(new org.apache.calcite.jdbc.JavaTypeFactoryImpl());
        RelDataTypeField field = rowType.getField("price", true, false);
        assertNotNull(field);
        assertTrue("Expected ScaledFloatType marker, got " + field.getType().getClass(), field.getType() instanceof ScaledFloatType);
        assertEquals(SqlTypeName.BIGINT, field.getType().getSqlTypeName());
        assertEquals(10.0, ((ScaledFloatType) field.getType()).getScalingFactor(), 0.0);
    }

    /** Factor-10 and factor-100 ScaledFloatType instances are digest-distinct (canonicalization trap). */
    public void testScaledFloatDigestDistinctness() throws Exception {
        String mapping = "{\"properties\":{"
            + "\"price10\":{\"type\":\"scaled_float\",\"scaling_factor\":10},"
            + "\"price100\":{\"type\":\"scaled_float\",\"scaling_factor\":100}"
            + "}}";
        ClusterState clusterState = buildClusterStateRaw("sf_digest_idx", mapping);
        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterState);
        Table table = schema.getTable("sf_digest_idx");
        RelDataType rowType = table.getRowType(new org.apache.calcite.jdbc.JavaTypeFactoryImpl());
        RelDataType type10 = rowType.getField("price10", true, false).getType();
        RelDataType type100 = rowType.getField("price100", true, false).getType();
        assertNotEquals("factor-10 and factor-100 must be distinct type objects", type10, type100);
    }

    /** Missing scaling_factor excludes the field from schema (does not throw). */
    public void testScaledFloatMissingFactorExcludesField() throws Exception {
        String mapping = "{\"properties\":{\"price\":{\"type\":\"scaled_float\"}}}";
        ClusterState clusterState = buildClusterStateRaw("sf_no_factor_idx", mapping);
        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterState);
        Table table = schema.getTable("sf_no_factor_idx");
        assertNotNull(table);
        RelDataType rowType = table.getRowType(new org.apache.calcite.jdbc.JavaTypeFactoryImpl());
        assertNull("Field without scaling_factor must be excluded", rowType.getField("price", true, false));
    }

    /** Zero scaling_factor excludes the field from schema. */
    public void testScaledFloatZeroFactorExcludesField() throws Exception {
        String mapping = "{\"properties\":{\"price\":{\"type\":\"scaled_float\",\"scaling_factor\":0}}}";
        ClusterState clusterState = buildClusterStateRaw("sf_zero_idx", mapping);
        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterState);
        Table table = schema.getTable("sf_zero_idx");
        RelDataType rowType = table.getRowType(new org.apache.calcite.jdbc.JavaTypeFactoryImpl());
        assertNull("Field with zero scaling_factor must be excluded", rowType.getField("price", true, false));
    }

    /** Negative scaling_factor excludes the field from schema. */
    public void testScaledFloatNegativeFactorExcludesField() throws Exception {
        String mapping = "{\"properties\":{\"price\":{\"type\":\"scaled_float\",\"scaling_factor\":-5}}}";
        ClusterState clusterState = buildClusterStateRaw("sf_neg_idx", mapping);
        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterState);
        Table table = schema.getTable("sf_neg_idx");
        RelDataType rowType = table.getRowType(new org.apache.calcite.jdbc.JavaTypeFactoryImpl());
        assertNull("Field with negative scaling_factor must be excluded", rowType.getField("price", true, false));
    }

    /** String-valued scaling_factor (e.g. "10") is accepted and parsed. */
    public void testScaledFloatStringFactorAccepted() throws Exception {
        String mapping = "{\"properties\":{\"price\":{\"type\":\"scaled_float\",\"scaling_factor\":\"10\"}}}";
        ClusterState clusterState = buildClusterStateRaw("sf_str_idx", mapping);
        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterState);
        Table table = schema.getTable("sf_str_idx");
        RelDataType rowType = table.getRowType(new org.apache.calcite.jdbc.JavaTypeFactoryImpl());
        RelDataTypeField field = rowType.getField("price", true, false);
        assertNotNull("String factor '10' must be accepted", field);
        assertTrue(field.getType() instanceof ScaledFloatType);
        assertEquals(10.0, ((ScaledFloatType) field.getType()).getScalingFactor(), 0.0);
    }

    // --- unsigned_long tests ---

    /** unsigned_long field produces an UnsignedLongType marker (BIGINT-backed). */
    public void testUnsignedLongFieldProducesUnsignedLongType() throws Exception {
        ClusterState clusterState = buildClusterState(Map.of("ul_idx", Map.of("counter", "unsigned_long")));
        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterState);
        Table table = schema.getTable("ul_idx");
        assertNotNull(table);
        RelDataType rowType = table.getRowType(new org.apache.calcite.jdbc.JavaTypeFactoryImpl());
        RelDataTypeField field = rowType.getField("counter", true, false);
        assertNotNull(field);
        assertTrue("Expected UnsignedLongType marker, got " + field.getType().getClass(), field.getType() instanceof UnsignedLongType);
        assertEquals(SqlTypeName.BIGINT, field.getType().getSqlTypeName());
    }

    /** UnsignedLongType digest is UNSIGNED_LONG, distinct from plain BIGINT. */
    public void testUnsignedLongDigestDistinctFromPlainBigint() throws Exception {
        ClusterState clusterState = buildClusterState(
            Map.of("mixed_long_idx", Map.of("signed_val", "long", "unsigned_val", "unsigned_long"))
        );
        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterState);
        Table table = schema.getTable("mixed_long_idx");
        RelDataType rowType = table.getRowType(new org.apache.calcite.jdbc.JavaTypeFactoryImpl());
        RelDataType signedType = rowType.getField("signed_val", true, false).getType();
        RelDataType unsignedType = rowType.getField("unsigned_val", true, false).getType();
        // Both report BIGINT
        assertEquals(SqlTypeName.BIGINT, signedType.getSqlTypeName());
        assertEquals(SqlTypeName.BIGINT, unsignedType.getSqlTypeName());
        // But they are distinguishable via instanceof
        assertFalse("plain long must NOT be UnsignedLongType", signedType instanceof UnsignedLongType);
        assertTrue("unsigned_long must be UnsignedLongType", unsignedType instanceof UnsignedLongType);
    }

    // --- helpers ---

    private void assertFieldType(RelDataType rowType, String fieldName, SqlTypeName expectedType) {
        RelDataTypeField field = rowType.getField(fieldName, true, false);
        assertNotNull("Field '" + fieldName + "' should exist", field);
        assertEquals("Field '" + fieldName + "' should have type " + expectedType, expectedType, field.getType().getSqlTypeName());
    }

    /**
     * Aliases over indices show up in the schema as their own table so the Calcite validator
     * can resolve {@code SELECT * FROM alias}. The exposed row type is the field-union of every
     * backing index — fields absent from some indices appear as nullable columns in the alias
     * row type, and {@code OpenSearchTableScanRule} null-fills those columns at scan time.
     */
    public void testBuildSchemaExposesAliasAsTable() throws Exception {
        IndexMetadata a = IndexMetadata.builder("bank_a")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putMapping("{\"properties\":{\"age\":{\"type\":\"long\"}}}")
            .putAlias(AliasMetadata.builder("bank_all").build())
            .build();
        IndexMetadata b = IndexMetadata.builder("bank_b")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putMapping("{\"properties\":{\"age\":{\"type\":\"long\"}}}")
            .putAlias(AliasMetadata.builder("bank_all").build())
            .build();
        ClusterState state = ClusterState.builder(new ClusterName("test"))
            .metadata(Metadata.builder().put(a, false).put(b, false).build())
            .build();

        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(state);

        assertNotNull("alias table present", schema.getTable("bank_all"));
        assertNotNull("backing concrete index still present", schema.getTable("bank_a"));
        RelDataType rowType = schema.getTable("bank_all").getRowType(new org.apache.calcite.jdbc.JavaTypeFactoryImpl());
        assertFieldType(rowType, "age", SqlTypeName.BIGINT);
    }

    /**
     * A single wildcard source ({@code test*}) resolves to one table whose row type is the union
     * of the matching concrete indices' supported fields. Mirrors the sql-plugin behavior where
     * {@code source=test*} resolves against the cluster's index expression.
     */
    public void testWildcardPatternResolvesToUnionedTable() throws Exception {
        ClusterState clusterState = buildClusterState(
            Map.of("test", Map.of("name", "keyword", "age", "long"), "test1", Map.of("name", "keyword", "alias_field", "keyword"))
        );

        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterState);

        Table table = schema.getTable("test*");
        assertNotNull("Wildcard 'test*' should resolve to a table", table);

        RelDataType rowType = table.getRowType(new org.apache.calcite.jdbc.JavaTypeFactoryImpl());
        assertFieldType(rowType, "name", SqlTypeName.VARCHAR);
        assertFieldType(rowType, "age", SqlTypeName.BIGINT);
        assertFieldType(rowType, "alias_field", SqlTypeName.VARCHAR);
    }

    /**
     * A comma-separated multi-source ({@code bank,test}) resolves to one table unioning both
     * indices' fields — the shape {@code source=a, b} produces after Relation comma-joins names.
     */
    public void testCommaSeparatedSourcesResolveToUnionedTable() throws Exception {
        ClusterState clusterState = buildClusterState(Map.of("bank", Map.of("balance", "long"), "test", Map.of("age", "long")));

        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterState);

        Table table = schema.getTable("bank,test");
        assertNotNull("Comma list 'bank,test' should resolve to a table", table);

        RelDataType rowType = table.getRowType(new org.apache.calcite.jdbc.JavaTypeFactoryImpl());
        assertFieldType(rowType, "balance", SqlTypeName.BIGINT);
        assertFieldType(rowType, "age", SqlTypeName.BIGINT);
    }

    /**
     * Comma-separated sources with field-name conflict: first-resolved-wins semantics. The
     * field 'age' appears as long in bank and keyword in test — whichever index the resolver
     * returns first determines the type. Both are valid; the key invariant is that the field
     * exists and unique-per-index fields from both indices appear in the union.
     */
    public void testCommaSeparatedSourcesFieldConflictPicksOne() throws Exception {
        IndexMetadata idx1 = buildIndexMetadata("bank", Map.of("age", "long", "name", "keyword"));
        IndexMetadata idx2 = buildIndexMetadata("test", Map.of("age", "keyword", "score", "double"));
        Metadata metadata = Metadata.builder().put(idx1, false).put(idx2, false).build();
        ClusterState clusterState = ClusterState.builder(new ClusterName("test")).metadata(metadata).build();

        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterState);
        Table table = schema.getTable("bank,test");
        assertNotNull("Comma-separated expression must resolve", table);

        RelDataType rowType = table.getRowType(new org.apache.calcite.jdbc.JavaTypeFactoryImpl());
        // 'age' exists — could be BIGINT or VARCHAR depending on resolver order
        RelDataTypeField ageField = rowType.getField("age", true, false);
        assertNotNull("Conflicting field 'age' must still appear in union", ageField);
        assertTrue(
            "age must be one of the two types",
            ageField.getType().getSqlTypeName() == SqlTypeName.BIGINT || ageField.getType().getSqlTypeName() == SqlTypeName.VARCHAR
        );
        // unique fields from each index must appear
        assertFieldType(rowType, "name", SqlTypeName.VARCHAR);
        assertFieldType(rowType, "score", SqlTypeName.DOUBLE);
    }

    /**
     * A pattern matching no index yields no table, so Calcite surfaces a clean "table not found"
     * at plan time rather than an empty-row-type table.
     */
    public void testNonMatchingPatternYieldsNoTable() throws Exception {
        ClusterState clusterState = buildClusterState(Map.of("test", Map.of("age", "long")));

        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterState);

        assertNull("Non-matching pattern 'nonexistent*' should resolve to no table", schema.getTable("nonexistent*"));
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
        // Preserve the original case of unquoted identifiers so both table names and column names
        // reach the validator as-typed. Default Calcite parsing uppercases unquoted identifiers
        // (Lex.ORACLE), which would force a case-insensitive validator + getTableNames()
        // enumeration — incompatible with the lazy schema (table names aren't enumerable until
        // resolved). PPL hits the schema via RelBuilder.scan with the user-typed case directly;
        // this config mirrors that for SQL-based unit tests.
        FrameworkConfig config = Frameworks.newConfigBuilder()
            .parserConfig(SqlParser.config().withUnquotedCasing(org.apache.calcite.avatica.util.Casing.UNCHANGED))
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

    // ===== D2: options-aware schema construction tests =====

    /**
     * Regression guard: an alias backed by two OPEN indices still yields a unioned row type
     * containing fields from both backings when using the new 3-arg buildSchema overload
     * with lenientExpandOpen (the default).
     */
    public void testAliasOverTwoOpenBackingsYieldsUnionedRowType() throws Exception {
        IndexMetadata a = IndexMetadata.builder("idx_a")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putMapping("{\"properties\":{\"field_a\":{\"type\":\"keyword\"}}}")
            .putAlias(AliasMetadata.builder("my_alias").build())
            .build();
        IndexMetadata b = IndexMetadata.builder("idx_b")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putMapping("{\"properties\":{\"field_b\":{\"type\":\"long\"}}}")
            .putAlias(AliasMetadata.builder("my_alias").build())
            .build();
        ClusterState state = ClusterState.builder(new ClusterName("test"))
            .metadata(Metadata.builder().put(a, false).put(b, false).build())
            .build();

        org.opensearch.cluster.metadata.IndexNameExpressionResolver resolver =
            new org.opensearch.cluster.metadata.IndexNameExpressionResolver(
                new org.opensearch.common.util.concurrent.ThreadContext(org.opensearch.common.settings.Settings.EMPTY)
            );
        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(state, resolver, IndicesOptions.lenientExpandOpen());

        Table table = schema.getTable("my_alias");
        assertNotNull("Alias over two OPEN backings must resolve", table);
        RelDataType rowType = table.getRowType(new org.apache.calcite.jdbc.JavaTypeFactoryImpl());
        assertFieldType(rowType, "field_a", SqlTypeName.VARCHAR);
        assertFieldType(rowType, "field_b", SqlTypeName.BIGINT);
    }

    /**
     * Phantom-column fix: an alias backed by one OPEN and one CLOSED index yields ONLY the
     * open backing's columns when resolved with lenientExpandOpen (wildcard states = OPEN only).
     */
    public void testAliasOverOpenAndClosedBackingExcludesClosedColumns() throws Exception {
        IndexMetadata open = IndexMetadata.builder("idx_open")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putMapping("{\"properties\":{\"open_field\":{\"type\":\"keyword\"}}}")
            .putAlias(AliasMetadata.builder("mixed_alias").build())
            .state(IndexMetadata.State.OPEN)
            .build();
        IndexMetadata closed = IndexMetadata.builder("idx_closed")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putMapping("{\"properties\":{\"closed_field\":{\"type\":\"long\"}}}")
            .putAlias(AliasMetadata.builder("mixed_alias").build())
            .state(IndexMetadata.State.CLOSE)
            .build();
        ClusterState state = ClusterState.builder(new ClusterName("test"))
            .metadata(Metadata.builder().put(open, false).put(closed, false).build())
            .build();

        org.opensearch.cluster.metadata.IndexNameExpressionResolver resolver =
            new org.opensearch.cluster.metadata.IndexNameExpressionResolver(
                new org.opensearch.common.util.concurrent.ThreadContext(org.opensearch.common.settings.Settings.EMPTY)
            );
        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(state, resolver, IndicesOptions.lenientExpandOpen());

        Table table = schema.getTable("mixed_alias");
        assertNotNull("Alias must still resolve when at least one backing is open", table);
        RelDataType rowType = table.getRowType(new org.apache.calcite.jdbc.JavaTypeFactoryImpl());
        assertFieldType(rowType, "open_field", SqlTypeName.VARCHAR);
        assertNull("Closed backing's column must NOT appear (phantom-column fix)", rowType.getField("closed_field", true, false));
    }

    /**
     * Phantom-column invariant: even when IndicesOptions requests expandWildcardsClosed, the
     * ALIAS short-circuit filters to State.OPEN unconditionally. A closed backing's fields must
     * NOT appear in the row type because the planner (IndexResolution.resolveAlias) never
     * targets closed indices — including them produces phantom columns that validate but
     * null-fill at execution. The schema's column set must never exceed the union of mappings
     * of the indices the planner will actually target.
     */
    public void testAliasWithExpandClosedOptionsStillExcludesClosedBacking() throws Exception {
        IndexMetadata open = IndexMetadata.builder("idx_open")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putMapping("{\"properties\":{\"open_field\":{\"type\":\"keyword\"}}}")
            .putAlias(AliasMetadata.builder("both_alias").build())
            .state(IndexMetadata.State.OPEN)
            .build();
        IndexMetadata closed = IndexMetadata.builder("idx_closed")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putMapping("{\"properties\":{\"closed_field\":{\"type\":\"long\"}}}")
            .putAlias(AliasMetadata.builder("both_alias").build())
            .state(IndexMetadata.State.CLOSE)
            .build();
        ClusterState state = ClusterState.builder(new ClusterName("test"))
            .metadata(Metadata.builder().put(open, false).put(closed, false).build())
            .build();

        // Options that expand both open AND closed
        IndicesOptions expandBoth = IndicesOptions.fromOptions(
            true,  // ignoreUnavailable
            true,  // allowNoIndices
            true,  // expandWildcardsOpen
            true   // expandWildcardsClosed
        );

        org.opensearch.cluster.metadata.IndexNameExpressionResolver resolver =
            new org.opensearch.cluster.metadata.IndexNameExpressionResolver(
                new org.opensearch.common.util.concurrent.ThreadContext(org.opensearch.common.settings.Settings.EMPTY)
            );
        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(state, resolver, expandBoth);

        Table table = schema.getTable("both_alias");
        assertNotNull("Alias must resolve with expandBoth options", table);
        RelDataType rowType = table.getRowType(new org.apache.calcite.jdbc.JavaTypeFactoryImpl());
        assertFieldType(rowType, "open_field", SqlTypeName.VARCHAR);
        assertNull(
            "Closed backing's column must NOT appear even with expandWildcardsClosed (phantom-column invariant)",
            rowType.getField("closed_field", true, false)
        );
    }

    /**
     * Wildcard with strict options (no ALLOW_NO_INDICES, no IGNORE_UNAVAILABLE) vs lenient:
     * strict options pass through to the resolver and may produce different results. Here we
     * verify that the options parameter IS actually threaded to the resolver's concreteIndexNames
     * call by checking that expandWildcardsClosed=true includes a closed index in the wildcard
     * resolution while lenientExpandOpen (open-only) does not.
     */
    public void testWildcardResolutionRespectsOptionsParameter() throws Exception {
        IndexMetadata open = IndexMetadata.builder("wc_open")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putMapping("{\"properties\":{\"open_col\":{\"type\":\"keyword\"}}}")
            .state(IndexMetadata.State.OPEN)
            .build();
        IndexMetadata closed = IndexMetadata.builder("wc_closed")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putMapping("{\"properties\":{\"closed_col\":{\"type\":\"long\"}}}")
            .state(IndexMetadata.State.CLOSE)
            .build();
        ClusterState state = ClusterState.builder(new ClusterName("test"))
            .metadata(Metadata.builder().put(open, false).put(closed, false).build())
            .build();

        org.opensearch.cluster.metadata.IndexNameExpressionResolver resolver =
            new org.opensearch.cluster.metadata.IndexNameExpressionResolver(
                new org.opensearch.common.util.concurrent.ThreadContext(org.opensearch.common.settings.Settings.EMPTY)
            );

        // lenientExpandOpen — wildcard states = OPEN only
        SchemaPlus schemaLenient = OpenSearchSchemaBuilder.buildSchema(state, resolver, IndicesOptions.lenientExpandOpen());
        Table tableLenient = schemaLenient.getTable("wc*");
        assertNotNull("wc* must resolve with lenient options", tableLenient);
        RelDataType lenientRow = tableLenient.getRowType(new org.apache.calcite.jdbc.JavaTypeFactoryImpl());
        assertFieldType(lenientRow, "open_col", SqlTypeName.VARCHAR);
        assertNull("Closed index must NOT appear under lenient options", lenientRow.getField("closed_col", true, false));

        // expandBoth — wildcard states = OPEN + CLOSED
        IndicesOptions expandBoth = IndicesOptions.fromOptions(true, true, true, true);
        SchemaPlus schemaBoth = OpenSearchSchemaBuilder.buildSchema(state, resolver, expandBoth);
        Table tableBoth = schemaBoth.getTable("wc*");
        assertNotNull("wc* must resolve with expandBoth options", tableBoth);
        RelDataType bothRow = tableBoth.getRowType(new org.apache.calcite.jdbc.JavaTypeFactoryImpl());
        assertFieldType(bothRow, "open_col", SqlTypeName.VARCHAR);
        assertFieldType(bothRow, "closed_col", SqlTypeName.BIGINT);
    }

    /**
     * Delegation regression guard: the 1-arg and 2-arg buildSchema overloads behave identically
     * to the 3-arg overload with lenientExpandOpen, confirming correct delegation.
     */
    public void testOneArgAndTwoArgOverloadsDelegateLenient() throws Exception {
        IndexMetadata open = IndexMetadata.builder("deleg_open")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putMapping("{\"properties\":{\"x\":{\"type\":\"keyword\"}}}")
            .putAlias(AliasMetadata.builder("deleg_alias").build())
            .state(IndexMetadata.State.OPEN)
            .build();
        IndexMetadata closed = IndexMetadata.builder("deleg_closed")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putMapping("{\"properties\":{\"y\":{\"type\":\"long\"}}}")
            .putAlias(AliasMetadata.builder("deleg_alias").build())
            .state(IndexMetadata.State.CLOSE)
            .build();
        ClusterState state = ClusterState.builder(new ClusterName("test"))
            .metadata(Metadata.builder().put(open, false).put(closed, false).build())
            .build();

        // 1-arg overload
        SchemaPlus schema1 = OpenSearchSchemaBuilder.buildSchema(state);
        Table table1 = schema1.getTable("deleg_alias");
        assertNotNull("1-arg overload must resolve alias", table1);
        RelDataType row1 = table1.getRowType(new org.apache.calcite.jdbc.JavaTypeFactoryImpl());
        assertFieldType(row1, "x", SqlTypeName.VARCHAR);
        assertNull("1-arg overload must exclude closed backing (lenient default)", row1.getField("y", true, false));

        // 2-arg overload
        org.opensearch.cluster.metadata.IndexNameExpressionResolver resolver =
            new org.opensearch.cluster.metadata.IndexNameExpressionResolver(
                new org.opensearch.common.util.concurrent.ThreadContext(org.opensearch.common.settings.Settings.EMPTY)
            );
        SchemaPlus schema2 = OpenSearchSchemaBuilder.buildSchema(state, resolver);
        Table table2 = schema2.getTable("deleg_alias");
        assertNotNull("2-arg overload must resolve alias", table2);
        RelDataType row2 = table2.getRowType(new org.apache.calcite.jdbc.JavaTypeFactoryImpl());
        assertFieldType(row2, "x", SqlTypeName.VARCHAR);
        assertNull("2-arg overload must exclude closed backing (lenient default)", row2.getField("y", true, false));
    }

    /**
     * Falsifiable pair proving expand_wildcards controls hidden-index inclusion: a wildcard
     * resolved with default options (open, not hidden) excludes the hidden index's unique field,
     * while the same wildcard resolved with expand_hidden=true includes it. If both assertions
     * produce the same result, expand_wildcards is not being honoured.
     */
    public void testExpandWildcardsHiddenControlsHiddenIndexInclusion() throws Exception {
        IndexMetadata visible = IndexMetadata.builder("hw_visible")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putMapping("{\"properties\":{\"visible_field\":{\"type\":\"keyword\"}}}")
            .state(IndexMetadata.State.OPEN)
            .build();
        IndexMetadata hidden = IndexMetadata.builder("hw_hidden")
            .settings(settings(Version.CURRENT).put("index.hidden", true))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putMapping("{\"properties\":{\"hidden_field\":{\"type\":\"long\"}}}")
            .state(IndexMetadata.State.OPEN)
            .build();
        ClusterState state = ClusterState.builder(new ClusterName("test"))
            .metadata(Metadata.builder().put(visible, false).put(hidden, false).build())
            .build();

        org.opensearch.cluster.metadata.IndexNameExpressionResolver resolver =
            new org.opensearch.cluster.metadata.IndexNameExpressionResolver(
                new org.opensearch.common.util.concurrent.ThreadContext(org.opensearch.common.settings.Settings.EMPTY)
            );

        // Default options: open indices only, hidden NOT expanded
        IndicesOptions defaultOpen = IndicesOptions.fromOptions(true, true, true, false, false);
        SchemaPlus schemaDefault = OpenSearchSchemaBuilder.buildSchema(state, resolver, defaultOpen);
        Table tableDefault = schemaDefault.getTable("hw_*");
        // The wildcard must match the visible index but skip the hidden one
        assertNotNull("hw_* must resolve with default options (visible index matches)", tableDefault);
        RelDataType rowDefault = tableDefault.getRowType(new org.apache.calcite.jdbc.JavaTypeFactoryImpl());
        assertFieldType(rowDefault, "visible_field", SqlTypeName.VARCHAR);
        assertNull(
            "Default options (hidden=false) must NOT include hidden index's field",
            rowDefault.getField("hidden_field", true, false)
        );

        // Expand hidden: open + hidden
        IndicesOptions withHidden = IndicesOptions.fromOptions(true, true, true, false, true);
        SchemaPlus schemaHidden = OpenSearchSchemaBuilder.buildSchema(state, resolver, withHidden);
        Table tableHidden = schemaHidden.getTable("hw_*");
        assertNotNull("hw_* must resolve with hidden options", tableHidden);
        RelDataType rowHidden = tableHidden.getRowType(new org.apache.calcite.jdbc.JavaTypeFactoryImpl());
        assertFieldType(rowHidden, "visible_field", SqlTypeName.VARCHAR);
        assertFieldType(rowHidden, "hidden_field", SqlTypeName.BIGINT);

        // Falsifiability check: the two row types must differ
        assertNotEquals(
            "The two row types must differ — otherwise expand_wildcards is not controlling hidden-index inclusion",
            rowDefault.getFieldCount(),
            rowHidden.getFieldCount()
        );
    }
}
