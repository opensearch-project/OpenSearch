/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.eqe.calcite;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link OpenSearchSchemaBuilder}.
 */
public class OpenSearchSchemaBuilderTests extends OpenSearchTestCase {

    private ClusterService mockClusterService(Metadata metadata) {
        ClusterService clusterService = mock(ClusterService.class);
        ClusterState clusterState = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        return clusterService;
    }

    private IndexMetadata buildIndex(String name, String mappingJson) {
        try {
            IndexMetadata.Builder builder = IndexMetadata.builder(name)
                .settings(Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .build())
                .primaryTerm(0, 1);
            if (mappingJson != null) {
                builder.putMapping(mappingJson);
            }
            return builder.build();
        } catch (java.io.IOException e) {
            throw new RuntimeException(e);
        }
    }

    private RelOptTable lookupTable(CalciteCatalogReader reader, String tableName) {
        return reader.getTable(List.of(tableName));
    }

    public void testSingleIndexRegistered() {
        IndexMetadata idx = buildIndex("users", "{ \"properties\": { \"name\": { \"type\": \"keyword\" } } }");
        Metadata metadata = Metadata.builder().put(idx, false).build();
        ClusterService cs = mockClusterService(metadata);

        CalciteCatalogReader reader = new OpenSearchSchemaBuilder(cs).buildCatalogReader();

        assertNotNull("Table 'users' should be registered", lookupTable(reader, "users"));
    }

    public void testMultipleIndicesRegistered() {
        IndexMetadata users = buildIndex("users", "{ \"properties\": { \"name\": { \"type\": \"keyword\" } } }");
        IndexMetadata orders = buildIndex("orders", "{ \"properties\": { \"total\": { \"type\": \"double\" } } }");
        Metadata metadata = Metadata.builder().put(users, false).put(orders, false).build();
        ClusterService cs = mockClusterService(metadata);

        CalciteCatalogReader reader = new OpenSearchSchemaBuilder(cs).buildCatalogReader();

        assertNotNull("Table 'users' should be registered", lookupTable(reader, "users"));
        assertNotNull("Table 'orders' should be registered", lookupTable(reader, "orders"));
    }

    public void testSystemIndicesSkipped() {
        IndexMetadata visible = buildIndex("users", "{ \"properties\": { \"name\": { \"type\": \"keyword\" } } }");
        IndexMetadata system = buildIndex(".kibana", "{ \"properties\": { \"config\": { \"type\": \"text\" } } }");
        Metadata metadata = Metadata.builder().put(visible, false).put(system, false).build();
        ClusterService cs = mockClusterService(metadata);

        CalciteCatalogReader reader = new OpenSearchSchemaBuilder(cs).buildCatalogReader();

        assertNotNull("Table 'users' should be registered", lookupTable(reader, "users"));
        assertNull("System index '.kibana' should be skipped", lookupTable(reader, ".kibana"));
    }

    public void testNoIndices() {
        Metadata metadata = Metadata.builder().build();
        ClusterService cs = mockClusterService(metadata);

        CalciteCatalogReader reader = new OpenSearchSchemaBuilder(cs).buildCatalogReader();

        assertNull("Non-existent table should return null", lookupTable(reader, "anything"));
    }

    public void testNonExistentTableReturnsNull() {
        IndexMetadata idx = buildIndex("users", "{ \"properties\": { \"name\": { \"type\": \"keyword\" } } }");
        Metadata metadata = Metadata.builder().put(idx, false).build();
        ClusterService cs = mockClusterService(metadata);

        CalciteCatalogReader reader = new OpenSearchSchemaBuilder(cs).buildCatalogReader();

        assertNull("Non-existent table should return null", lookupTable(reader, "orders"));
    }

    // --- Field type mapping ---

    public void testKeywordMapsToVarchar() {
        assertFieldType("keyword", SqlTypeName.VARCHAR);
    }

    public void testTextMapsToVarchar() {
        assertFieldType("text", SqlTypeName.VARCHAR);
    }

    public void testLongMapsToBigint() {
        assertFieldType("long", SqlTypeName.BIGINT);
    }

    public void testIntegerMapsToInteger() {
        assertFieldType("integer", SqlTypeName.INTEGER);
    }

    public void testShortMapsToSmallint() {
        assertFieldType("short", SqlTypeName.SMALLINT);
    }

    public void testByteMapsToTinyint() {
        assertFieldType("byte", SqlTypeName.TINYINT);
    }

    public void testDoubleMapsToDouble() {
        assertFieldType("double", SqlTypeName.DOUBLE);
    }

    public void testFloatMapsToFloat() {
        assertFieldType("float", SqlTypeName.FLOAT);
    }

    public void testHalfFloatMapsToFloat() {
        assertFieldType("half_float", SqlTypeName.FLOAT);
    }

    public void testScaledFloatMapsToDouble() {
        assertFieldType("scaled_float", SqlTypeName.DOUBLE);
    }

    public void testBooleanMapsToBoolean() {
        assertFieldType("boolean", SqlTypeName.BOOLEAN);
    }

    public void testDateMapsToTimestamp() {
        assertFieldType("date", SqlTypeName.TIMESTAMP);
    }

    public void testDateNanosMapsToTimestamp() {
        assertFieldType("date_nanos", SqlTypeName.TIMESTAMP);
    }

    public void testBinaryMapsToVarbinary() {
        assertFieldType("binary", SqlTypeName.VARBINARY);
    }

    public void testUnsignedLongMapsToDecimal() {
        assertFieldType("unsigned_long", SqlTypeName.DECIMAL);
    }

    public void testUnknownTypeMapsToVarchar() {
        assertFieldType("ip", SqlTypeName.VARCHAR);
        assertFieldType("geo_point", SqlTypeName.VARCHAR);
    }

    private void assertFieldType(String osType, SqlTypeName expectedSqlType) {
        String mapping = "{ \"properties\": { \"field\": { \"type\": \"" + osType + "\" } } }";
        IndexMetadata idx = buildIndex("test_idx", mapping);
        Metadata metadata = Metadata.builder().put(idx, false).build();
        ClusterService cs = mockClusterService(metadata);

        CalciteCatalogReader reader = new OpenSearchSchemaBuilder(cs).buildCatalogReader();
        RelOptTable table = lookupTable(reader, "test_idx");
        assertNotNull(table);

        RelDataType rowType = table.getRowType();
        assertEquals(1, rowType.getFieldCount());
        RelDataTypeField field = rowType.getField("field", true, false);
        assertNotNull("Field 'field' should exist", field);
        assertEquals(expectedSqlType, field.getType().getSqlTypeName());
        assertTrue("Fields should be nullable", field.getType().isNullable());
    }

    public void testMultipleFieldTypes() {
        String mapping = "{ \"properties\": {"
            + "\"name\": { \"type\": \"keyword\" },"
            + "\"age\": { \"type\": \"long\" },"
            + "\"score\": { \"type\": \"double\" },"
            + "\"active\": { \"type\": \"boolean\" }"
            + "} }";
        IndexMetadata idx = buildIndex("users", mapping);
        Metadata metadata = Metadata.builder().put(idx, false).build();
        ClusterService cs = mockClusterService(metadata);

        CalciteCatalogReader reader = new OpenSearchSchemaBuilder(cs).buildCatalogReader();
        RelOptTable table = lookupTable(reader, "users");
        assertNotNull(table);

        RelDataType rowType = table.getRowType();
        assertEquals(4, rowType.getFieldCount());
        assertEquals(SqlTypeName.VARCHAR, rowType.getField("name", true, false).getType().getSqlTypeName());
        assertEquals(SqlTypeName.BIGINT, rowType.getField("age", true, false).getType().getSqlTypeName());
        assertEquals(SqlTypeName.DOUBLE, rowType.getField("score", true, false).getType().getSqlTypeName());
        assertEquals(SqlTypeName.BOOLEAN, rowType.getField("active", true, false).getType().getSqlTypeName());
    }

    public void testNestedObjectFieldsFlattened() {
        String mapping = "{ \"properties\": {"
            + "\"address\": { \"properties\": {"
            + "  \"city\": { \"type\": \"keyword\" },"
            + "  \"zip\": { \"type\": \"integer\" }"
            + "} }"
            + "} }";
        IndexMetadata idx = buildIndex("users", mapping);
        Metadata metadata = Metadata.builder().put(idx, false).build();
        ClusterService cs = mockClusterService(metadata);

        CalciteCatalogReader reader = new OpenSearchSchemaBuilder(cs).buildCatalogReader();
        RelOptTable table = lookupTable(reader, "users");
        assertNotNull(table);

        RelDataType rowType = table.getRowType();
        assertEquals(2, rowType.getFieldCount());
        assertNotNull("Should have 'address.city'", rowType.getField("address.city", true, false));
        assertNotNull("Should have 'address.zip'", rowType.getField("address.zip", true, false));
        assertEquals(SqlTypeName.VARCHAR, rowType.getField("address.city", true, false).getType().getSqlTypeName());
        assertEquals(SqlTypeName.INTEGER, rowType.getField("address.zip", true, false).getType().getSqlTypeName());
    }

    public void testDeeplyNestedObjectFields() {
        String mapping = "{ \"properties\": {"
            + "\"a\": { \"properties\": {"
            + "  \"b\": { \"properties\": {"
            + "    \"c\": { \"type\": \"long\" }"
            + "  } }"
            + "} }"
            + "} }";
        IndexMetadata idx = buildIndex("deep", mapping);
        Metadata metadata = Metadata.builder().put(idx, false).build();
        ClusterService cs = mockClusterService(metadata);

        CalciteCatalogReader reader = new OpenSearchSchemaBuilder(cs).buildCatalogReader();
        RelOptTable table = lookupTable(reader, "deep");
        assertNotNull(table);

        RelDataType rowType = table.getRowType();
        assertEquals(1, rowType.getFieldCount());
        RelDataTypeField field = rowType.getField("a.b.c", true, false);
        assertNotNull("Should have 'a.b.c'", field);
        assertEquals(SqlTypeName.BIGINT, field.getType().getSqlTypeName());
    }

    public void testMixedTopLevelAndNestedFields() {
        String mapping = "{ \"properties\": {"
            + "\"name\": { \"type\": \"keyword\" },"
            + "\"address\": { \"properties\": {"
            + "  \"street\": { \"type\": \"text\" }"
            + "} },"
            + "\"age\": { \"type\": \"integer\" }"
            + "} }";
        IndexMetadata idx = buildIndex("users", mapping);
        Metadata metadata = Metadata.builder().put(idx, false).build();
        ClusterService cs = mockClusterService(metadata);

        CalciteCatalogReader reader = new OpenSearchSchemaBuilder(cs).buildCatalogReader();
        RelOptTable table = lookupTable(reader, "users");
        assertNotNull(table);

        RelDataType rowType = table.getRowType();
        assertEquals(3, rowType.getFieldCount());
        assertNotNull(rowType.getField("name", true, false));
        assertNotNull(rowType.getField("address.street", true, false));
        assertNotNull(rowType.getField("age", true, false));
    }

    public void testIndexWithNoMapping() {
        IndexMetadata idx = buildIndex("empty_idx", null);
        Metadata metadata = Metadata.builder().put(idx, false).build();
        ClusterService cs = mockClusterService(metadata);

        CalciteCatalogReader reader = new OpenSearchSchemaBuilder(cs).buildCatalogReader();
        RelOptTable table = lookupTable(reader, "empty_idx");
        assertNotNull("Table should still be registered", table);

        RelDataType rowType = table.getRowType();
        assertEquals("Should have no fields", 0, rowType.getFieldCount());
    }

    public void testIndexWithEmptyProperties() {
        IndexMetadata idx = buildIndex("empty_props", "{ \"properties\": {} }");
        Metadata metadata = Metadata.builder().put(idx, false).build();
        ClusterService cs = mockClusterService(metadata);

        CalciteCatalogReader reader = new OpenSearchSchemaBuilder(cs).buildCatalogReader();
        RelOptTable table = lookupTable(reader, "empty_props");
        assertNotNull(table);

        RelDataType rowType = table.getRowType();
        assertEquals("Should have no fields", 0, rowType.getFieldCount());
    }

    public void testAllFieldsAreNullable() {
        String mapping = "{ \"properties\": {"
            + "\"a\": { \"type\": \"keyword\" },"
            + "\"b\": { \"type\": \"long\" },"
            + "\"c\": { \"type\": \"double\" }"
            + "} }";
        IndexMetadata idx = buildIndex("test_idx", mapping);
        Metadata metadata = Metadata.builder().put(idx, false).build();
        ClusterService cs = mockClusterService(metadata);

        CalciteCatalogReader reader = new OpenSearchSchemaBuilder(cs).buildCatalogReader();
        RelOptTable table = lookupTable(reader, "test_idx");
        assertNotNull(table);

        for (RelDataTypeField field : table.getRowType().getFieldList()) {
            assertTrue("Field '" + field.getName() + "' should be nullable", field.getType().isNullable());
        }
    }

    public void testMultipleSystemIndicesAllSkipped() {
        IndexMetadata s1 = buildIndex(".security", "{ \"properties\": { \"x\": { \"type\": \"text\" } } }");
        IndexMetadata s2 = buildIndex(".tasks", "{ \"properties\": { \"y\": { \"type\": \"text\" } } }");
        IndexMetadata s3 = buildIndex(".opendistro", "{ \"properties\": { \"z\": { \"type\": \"text\" } } }");
        IndexMetadata visible = buildIndex("logs", "{ \"properties\": { \"msg\": { \"type\": \"text\" } } }");
        Metadata metadata = Metadata.builder()
            .put(s1, false).put(s2, false).put(s3, false).put(visible, false)
            .build();
        ClusterService cs = mockClusterService(metadata);

        CalciteCatalogReader reader = new OpenSearchSchemaBuilder(cs).buildCatalogReader();

        assertNull(lookupTable(reader, ".security"));
        assertNull(lookupTable(reader, ".tasks"));
        assertNull(lookupTable(reader, ".opendistro"));
        assertNotNull(lookupTable(reader, "logs"));
    }
}
