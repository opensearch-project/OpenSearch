/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package fe.ppl;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.fe.ppl.OpenSearchSchemaBuilder;
import org.opensearch.test.OpenSearchTestCase;

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
        assertFieldType(rowType, "ratio", SqlTypeName.FLOAT);
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
        assertFieldType(rowType, "address", SqlTypeName.VARCHAR);
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
        // no tables
        assertTrue("Schema should have no tables", schema.getTableNames().isEmpty());
    }

    /**
     * Test mapFieldType for all supported types.
     */
    public void testMapFieldTypeForAllSupportedTypes() {
        assertEquals(SqlTypeName.VARCHAR, OpenSearchSchemaBuilder.mapFieldType("keyword"));
        assertEquals(SqlTypeName.VARCHAR, OpenSearchSchemaBuilder.mapFieldType("text"));
        assertEquals(SqlTypeName.BIGINT, OpenSearchSchemaBuilder.mapFieldType("long"));
        assertEquals(SqlTypeName.INTEGER, OpenSearchSchemaBuilder.mapFieldType("integer"));
        assertEquals(SqlTypeName.SMALLINT, OpenSearchSchemaBuilder.mapFieldType("short"));
        assertEquals(SqlTypeName.TINYINT, OpenSearchSchemaBuilder.mapFieldType("byte"));
        assertEquals(SqlTypeName.DOUBLE, OpenSearchSchemaBuilder.mapFieldType("double"));
        assertEquals(SqlTypeName.FLOAT, OpenSearchSchemaBuilder.mapFieldType("float"));
        assertEquals(SqlTypeName.BOOLEAN, OpenSearchSchemaBuilder.mapFieldType("boolean"));
        assertEquals(SqlTypeName.TIMESTAMP, OpenSearchSchemaBuilder.mapFieldType("date"));
        assertEquals(SqlTypeName.VARCHAR, OpenSearchSchemaBuilder.mapFieldType("ip"));
    }

    /**
     * Test that unknown field types default to VARCHAR.
     */
    public void testUnknownFieldTypeDefaultsToVarchar() {
        assertEquals(SqlTypeName.VARCHAR, OpenSearchSchemaBuilder.mapFieldType("unknown_type"));
        assertEquals(SqlTypeName.VARCHAR, OpenSearchSchemaBuilder.mapFieldType("geo_point"));
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

    private IndexMetadata buildIndexMetadata(String indexName, Map<String, String> fieldTypes) throws Exception {
        StringBuilder mappingJson = new StringBuilder("{\"properties\":{");
        boolean first = true;
        for (Map.Entry<String, String> field : fieldTypes.entrySet()) {
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
}
