/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.backend.EngineBridge;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import io.substrait.isthmus.SqlToSubstrait;

/**
 * Tests for {@link DataFusionEngineInstance} — validates the JNI bridge
 * to DataFusionPlugin with Substrait plan generated via Isthmus SqlToSubstrait.
 */
public class DataFusionPluginEngineInstanceTests extends OpenSearchTestCase {

    private BufferAllocator allocator;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        allocator = new RootAllocator();
    }

    @Override
    public void tearDown() throws Exception {
        allocator.close();
        super.tearDown();
    }

    public void testHashJoinTwoInputs() throws Exception {
        // --- Build Substrait plan from SQL via Isthmus ---
        byte[] planBytes = buildSubstraitPlan("SELECT o.amount, c.name FROM orders o INNER JOIN customers c ON o.customer_id = c.id");

        // --- Build orders table: [customer_id: int, amount: double] ---
        Schema ordersSchema = new Schema(
            Arrays.asList(
                new Field("customer_id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("amount", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null)
            )
        );
        VectorSchemaRoot ordersRoot = VectorSchemaRoot.create(ordersSchema, allocator);
        ordersRoot.allocateNew();
        IntVector customerIdVec = (IntVector) ordersRoot.getVector("customer_id");
        Float8Vector amountVec = (Float8Vector) ordersRoot.getVector("amount");
        customerIdVec.setSafe(0, 1);
        amountVec.setSafe(0, 100.0);
        customerIdVec.setSafe(1, 2);
        amountVec.setSafe(1, 200.0);
        customerIdVec.setSafe(2, 3);
        amountVec.setSafe(2, 300.0);
        customerIdVec.setSafe(3, 1);
        amountVec.setSafe(3, 150.0);
        ordersRoot.setRowCount(4);

        // --- Build customers table: [id: int, name: varchar] ---
        Schema customersSchema = new Schema(
            Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)
            )
        );
        VectorSchemaRoot customersRoot = VectorSchemaRoot.create(customersSchema, allocator);
        customersRoot.allocateNew();
        IntVector idVec = (IntVector) customersRoot.getVector("id");
        VarCharVector nameVec = (VarCharVector) customersRoot.getVector("name");
        idVec.setSafe(0, 1);
        nameVec.setSafe(0, "Alice".getBytes(StandardCharsets.UTF_8));
        idVec.setSafe(1, 2);
        nameVec.setSafe(1, "Bob".getBytes(StandardCharsets.UTF_8));
        customersRoot.setRowCount(2);

        // --- Execute via DataFusionPlugin JNI ---
        DataFusionBridge bridge = new DataFusionBridge();
        EngineInstance engine = bridge.create(planBytes, allocator);

        engine.addBatch(0, ordersRoot);    // input 0 = orders (DFS left)
        engine.finishInput(0);

        engine.addBatch(1, customersRoot); // input 1 = customers (DFS right)
        engine.finishInput(1);

        VectorSchemaRoot result = engine.nextResult();

        // --- Verify: 3 rows (customer_id=3 has no match) ---
        assertNotNull("Result should not be null", result);
        assertEquals(2, result.getSchema().getFields().size());
        assertEquals(3, result.getRowCount());

        // Get column names from the result schema (DataFusionPlugin determines output names)
        String amountField = result.getSchema().getFields().get(0).getName();
        String nameField = result.getSchema().getFields().get(1).getName();

        // Collect actual rows into a set for order-independent comparison
        Set<String> actualRows = new HashSet<>();
        for (int i = 0; i < result.getRowCount(); i++) {
            Object amount = result.getVector(amountField).getObject(i);
            Object name = result.getVector(nameField).getObject(i);
            actualRows.add(amount + ":" + name);
        }
        Set<String> expectedRows = new HashSet<>(Arrays.asList("100.0:Alice", "200.0:Bob", "150.0:Alice"));
        assertEquals(expectedRows, actualRows);

        // No more results
        assertNull(engine.nextResult());

        // Clean up
        result.close();
        engine.close();
        ordersRoot.close();
        customersRoot.close();
    }

    public void testBridgeCreateReturnsEngineInstance() {
        DataFusionBridge bridge = new DataFusionBridge();
        // Minimal valid Substrait plan (empty)
        byte[] planBytes = io.substrait.proto.Plan.getDefaultInstance().toByteArray();
        EngineInstance engine = bridge.create(planBytes, allocator);
        assertNotNull(engine);
        engine.close();
    }

    public void testDefaultBridgeCreateThrows() {
        EngineBridge plainBridge = (serializedPlan, input, alloc) -> input;
        expectThrows(UnsupportedOperationException.class, () -> plainBridge.create(new byte[0], allocator));
    }

    /**
     * Build Substrait protobuf bytes from a SQL string using Isthmus.
     * Registers "orders" and "customers" tables in a Calcite catalog.
     */
    private byte[] buildSubstraitPlan(String sql) throws Exception {
        JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(true);
        SchemaPlus schemaPlus = rootSchema.plus();

        schemaPlus.add("orders", new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory tf) {
                return tf.builder()
                    .add("customer_id", tf.createSqlType(SqlTypeName.INTEGER))
                    .add("amount", tf.createSqlType(SqlTypeName.DOUBLE))
                    .build();
            }
        });

        schemaPlus.add("customers", new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory tf) {
                return tf.builder()
                    .add("id", tf.createSqlType(SqlTypeName.INTEGER))
                    .add("name", tf.createSqlType(SqlTypeName.VARCHAR))
                    .build();
            }
        });

        Properties props = new Properties();
        props.setProperty("caseSensitive", "false");
        CalciteConnectionConfig config = new CalciteConnectionConfigImpl(props);
        CalciteCatalogReader catalogReader = new CalciteCatalogReader(rootSchema, Collections.singletonList(""), typeFactory, config);

        SqlToSubstrait sqlToSubstrait = new SqlToSubstrait();
        io.substrait.proto.Plan protoPlan = sqlToSubstrait.execute(sql, catalogReader);
        return protoPlan.toByteArray();
    }
}
