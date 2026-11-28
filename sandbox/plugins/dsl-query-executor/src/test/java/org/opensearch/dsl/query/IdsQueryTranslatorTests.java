/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.query;

import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.index.query.IdsQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

/**
 * Unit tests for {@link IdsQueryTranslator}.
 */
public class IdsQueryTranslatorTests extends OpenSearchTestCase {

    private final IdsQueryTranslator translator = new IdsQueryTranslator();
    private final ConversionContext ctx = createContextWithId();

    public void testSingleId() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.idsQuery().addIds("doc1"), ctx);

        assertTrue("Expected RexCall, got: " + result.getClass(), result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.OTHER_FUNCTION, call.getKind());
        assertEquals("IDS", call.getOperator().getName());
        // Fieldless: only MAP('values.N', 'idN') operands, no field MAP
        assertEquals(1, call.getOperands().size());
    }

    public void testMultipleIds() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.idsQuery().addIds("doc1", "doc2", "doc3"), ctx);

        assertTrue("Expected RexCall, got: " + result.getClass(), result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals("IDS", call.getOperator().getName());
        // Fieldless: 3 value MAP operands
        assertEquals(3, call.getOperands().size());
    }

    public void testIdContainingComma() throws ConversionException {
        // Lossless round-trip: an id containing a comma must survive encoding
        String commaId = "doc,with,commas";
        RexNode result = translator.convert(QueryBuilders.idsQuery().addIds(commaId), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        // The value MAP at operand 0 must carry the exact id including commas (fieldless: no field operand)
        RexCall valueMap = (RexCall) call.getOperands().get(0);
        RexLiteral valueLiteral = (RexLiteral) valueMap.getOperands().get(1);
        assertEquals(commaId, valueLiteral.getValueAs(String.class));
    }

    public void testEmptyValuesProducesFalseLiteral() throws ConversionException {
        // Empty values array → match-nothing (mirrors IdsQueryBuilder.doRewrite)
        RexNode result = translator.convert(QueryBuilders.idsQuery(), ctx);

        assertTrue("Expected RexLiteral FALSE, got: " + result, result instanceof RexLiteral);
        RexLiteral literal = (RexLiteral) result;
        assertEquals(Boolean.FALSE, literal.getValueAs(Boolean.class));
    }

    public void testBoostThrowsConversionException() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.idsQuery().addIds("doc1").boost(2.0f), ctx)
        );
        assertTrue("Error must mention boost; got: " + ex.getMessage(), ex.getMessage().contains("boost"));
    }

    public void testQueryNameThrowsConversionException() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.idsQuery().addIds("doc1").queryName("named"), ctx)
        );
        assertTrue("Error must mention _name; got: " + ex.getMessage(), ex.getMessage().contains("_name"));
    }

    public void testDuplicateIdsAreDeduplicated() throws ConversionException {
        // IdsQueryBuilder.ids() returns a Set, so duplicates are collapsed before translation.
        RexNode result = translator.convert(QueryBuilders.idsQuery().addIds("doc1", "doc1", "doc2"), ctx);

        assertTrue("Expected RexCall, got: " + result.getClass(), result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals("IDS", call.getOperator().getName());
        // Only 2 unique ids → exactly 2 value MAP operands
        assertEquals(2, call.getOperands().size());

        // Extract the id values from each MAP('values.N', id) operand
        Set<String> ids = new HashSet<>();
        for (RexNode operand : call.getOperands()) {
            RexCall mapCall = (RexCall) operand;
            RexLiteral valueLiteral = (RexLiteral) mapCall.getOperands().get(1);
            ids.add(valueLiteral.getValueAs(String.class));
        }
        assertEquals(Set.of("doc1", "doc2"), ids);
    }

    public void testReportsCorrectQueryType() {
        assertEquals(IdsQueryBuilder.class, translator.getQueryType());
    }

    public void testLargeIdListProducesCorrectOperands() throws ConversionException {
        // Stress test: 100 ids to verify no Calcite operand-count ceiling is hit
        String[] ids = new String[100];
        for (int i = 0; i < 100; i++) {
            ids[i] = String.format(Locale.ROOT, "doc%03d", i);
        }
        RexNode result = translator.convert(QueryBuilders.idsQuery().addIds(ids), ctx);

        assertTrue("Expected RexCall, got: " + result.getClass(), result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals("IDS", call.getOperator().getName());
        assertEquals(100, call.getOperands().size());

        // Ids are sorted before emission; spot-check first and last operands
        RexCall firstMap = (RexCall) call.getOperands().get(0);
        RexLiteral firstKey = (RexLiteral) firstMap.getOperands().get(0);
        RexLiteral firstValue = (RexLiteral) firstMap.getOperands().get(1);
        assertEquals("values.0", firstKey.getValueAs(String.class));
        assertEquals("doc000", firstValue.getValueAs(String.class));

        RexCall lastMap = (RexCall) call.getOperands().get(99);
        RexLiteral lastKey = (RexLiteral) lastMap.getOperands().get(0);
        RexLiteral lastValue = (RexLiteral) lastMap.getOperands().get(1);
        assertEquals("values.99", lastKey.getValueAs(String.class));
        assertEquals("doc099", lastValue.getValueAs(String.class));
    }

    public void testIdsWithSpecialCharactersRoundTrip() throws ConversionException {
        // Character edge-case: single quote, non-ASCII, and space must survive translation
        String quoteId = "it's";
        String unicodeId = "\u00fc\u00f1\u00ee-id";
        String spaceId = "id with space";
        RexNode result = translator.convert(QueryBuilders.idsQuery().addIds(quoteId, unicodeId, spaceId), ctx);

        assertTrue("Expected RexCall, got: " + result.getClass(), result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals("IDS", call.getOperator().getName());
        assertEquals(3, call.getOperands().size());

        // Extract actual id values from MAP operands and verify exact strings survive
        Set<String> recoveredIds = new HashSet<>();
        for (RexNode operand : call.getOperands()) {
            RexCall mapCall = (RexCall) operand;
            RexLiteral valueLiteral = (RexLiteral) mapCall.getOperands().get(1);
            recoveredIds.add(valueLiteral.getValueAs(String.class));
        }
        assertEquals(Set.of(quoteId, unicodeId, spaceId), recoveredIds);
    }

    /** Creates a ConversionContext with a simple schema (no _id column needed). */
    private static ConversionContext createContextWithId() {
        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        HepPlanner planner = new HepPlanner(HepProgram.builder().build());
        RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));

        SchemaPlus schema = CalciteSchema.createRootSchema(true).plus();
        schema.add("test", new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory tf) {
                return tf.builder()
                    .add("name", tf.createTypeWithNullability(tf.createSqlType(SqlTypeName.VARCHAR), true))
                    .add("price", tf.createTypeWithNullability(tf.createSqlType(SqlTypeName.INTEGER), true))
                    .build();
            }
        });

        CalciteCatalogReader reader = new CalciteCatalogReader(
            CalciteSchema.from(schema),
            Collections.singletonList(""),
            typeFactory,
            new CalciteConnectionConfigImpl(new Properties())
        );
        RelOptTable table = Objects.requireNonNull(reader.getTable(List.of("test")));
        return new ConversionContext(new SearchSourceBuilder(), cluster, table);
    }
}
