/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.FieldType;
import org.opensearch.test.OpenSearchTestCase;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * Property-based test for multi-field extraction order preservation.
 *
 * <p>Feature: lucene-fulltext-serializers, Property 3: Multi-field extraction preserves all fields in order
 *
 * <p>For any MAP_VALUE_CONSTRUCTOR RexCall operand containing N >= 1 RexInputRef entries,
 * {@code extractFieldsFromRelevanceMap} shall return a list of exactly N field names, each matching
 * the field name resolved from the corresponding FieldStorageInfo entry, in the same order as they
 * appear in the operand.
 *
 * <p>Validates: Requirements 9.1, 9.2
 */
public class MultiFieldExtractionOrderPropertyTests extends OpenSearchTestCase {

    private static final int ITERATIONS = 100;

    private static final SqlFunction MULTI_MATCH_FUNCTION = new SqlFunction(
        "MULTI_MATCH",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BOOLEAN,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
    }

    /**
     * Property 3: Multi-field extraction preserves all fields in order (nested MAP literal path).
     *
     * Generate a random list of 1-10 field names, construct a mock MAP_VALUE_CONSTRUCTOR RexCall
     * with the nested MAP structure (field name literals at even indices, boost literals at odd indices),
     * invoke extractFieldsFromRelevanceMap, and assert the returned list matches expected field names
     * in exact order.
     */
    public void testExtractionPreservesOrderNestedMapLiteralPath() {
        for (int iter = 0; iter < ITERATIONS; iter++) {
            int fieldCount = randomIntBetween(1, 10);
            List<String> expectedFields = generateUniqueFieldNames(fieldCount);

            RexCall call = buildNestedMapLiteralCall(expectedFields);
            List<FieldStorageInfo> fieldStorage = List.of(); // not used in nested MAP literal path

            List<String> result = ConversionUtils.extractFieldsFromRelevanceMap(call, 0, fieldStorage);

            assertEquals("Iteration " + iter + ": field count mismatch", expectedFields.size(), result.size());
            for (int i = 0; i < expectedFields.size(); i++) {
                assertEquals("Iteration " + iter + ": field at index " + i + " mismatch", expectedFields.get(i), result.get(i));
            }
        }
    }

    /**
     * Property 3: Multi-field extraction preserves all fields in order (RexInputRef fallback path).
     *
     * Generate a random list of 1-10 field names, construct a mock MAP_VALUE_CONSTRUCTOR RexCall
     * with RexInputRef entries at odd indices and a corresponding FieldStorageInfo list,
     * invoke extractFieldsFromRelevanceMap, and assert the returned list matches expected field names
     * in exact order.
     */
    public void testExtractionPreservesOrderRexInputRefPath() {
        for (int iter = 0; iter < ITERATIONS; iter++) {
            int fieldCount = randomIntBetween(1, 10);
            List<String> expectedFields = generateUniqueFieldNames(fieldCount);

            List<FieldStorageInfo> fieldStorage = new ArrayList<>();
            for (String fieldName : expectedFields) {
                fieldStorage.add(new FieldStorageInfo(fieldName, "text", FieldType.TEXT, List.of(), List.of("lucene"), List.of(), false));
            }

            RexCall call = buildRexInputRefCall(fieldCount);

            List<String> result = ConversionUtils.extractFieldsFromRelevanceMap(call, 0, fieldStorage);

            assertEquals("Iteration " + iter + ": field count mismatch", expectedFields.size(), result.size());
            for (int i = 0; i < expectedFields.size(); i++) {
                assertEquals("Iteration " + iter + ": field at index " + i + " mismatch", expectedFields.get(i), result.get(i));
            }
        }
    }

    /**
     * Generates a list of unique random field names.
     */
    private List<String> generateUniqueFieldNames(int count) {
        List<String> names = new ArrayList<>();
        while (names.size() < count) {
            String candidate = randomAlphaOfLengthBetween(1, 50);
            if (names.contains(candidate) == false) {
                names.add(candidate);
            }
        }
        return names;
    }

    /**
     * Builds a mock RexCall using the nested MAP literal structure:
     * MULTI_MATCH(MAP('fields', MAP('field1', 1.0, 'field2', 1.0, ...)), MAP('query', 'text'))
     *
     * The nested MAP has alternating field name literals (even indices) and boost literals (odd indices).
     */
    private RexCall buildNestedMapLiteralCall(List<String> fieldNames) {
        RelDataType doubleType = typeFactory.createSqlType(SqlTypeName.DOUBLE);

        // Build nested MAP operands: field1, boost1, field2, boost2, ...
        List<RexNode> nestedMapOperands = new ArrayList<>();
        for (String field : fieldNames) {
            nestedMapOperands.add(rexBuilder.makeLiteral(field));
            nestedMapOperands.add(rexBuilder.makeExactLiteral(new BigDecimal("1.0"), doubleType));
        }

        // Nested MAP: MAP_VALUE_CONSTRUCTOR(field1, 1.0, field2, 1.0, ...)
        RexNode nestedMap = rexBuilder.makeCall(SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR, nestedMapOperands.toArray(new RexNode[0]));

        // Outer MAP: MAP('fields', nestedMap)
        RexNode outerMap = rexBuilder.makeCall(SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR, rexBuilder.makeLiteral("fields"), nestedMap);

        // Query MAP: MAP('query', 'text')
        RexNode queryMap = rexBuilder.makeCall(
            SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
            rexBuilder.makeLiteral("query"),
            rexBuilder.makeLiteral("test query")
        );

        return (RexCall) rexBuilder.makeCall(MULTI_MATCH_FUNCTION, outerMap, queryMap);
    }

    /**
     * Builds a mock RexCall using the RexInputRef fallback structure:
     * MULTI_MATCH(MAP('field', $0, 'field', $1, ...), MAP('query', 'text'))
     *
     * The MAP has alternating key literals ('field') at even indices and RexInputRef at odd indices.
     */
    private RexCall buildRexInputRefCall(int fieldCount) {
        RelDataType varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR);

        // Build MAP operands: 'field', $0, 'field', $1, ...
        List<RexNode> mapOperands = new ArrayList<>();
        for (int i = 0; i < fieldCount; i++) {
            mapOperands.add(rexBuilder.makeLiteral("field"));
            mapOperands.add(rexBuilder.makeInputRef(varcharType, i));
        }

        RexNode fieldMap = rexBuilder.makeCall(SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR, mapOperands.toArray(new RexNode[0]));

        // Query MAP: MAP('query', 'text')
        RexNode queryMap = rexBuilder.makeCall(
            SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
            rexBuilder.makeLiteral("query"),
            rexBuilder.makeLiteral("test query")
        );

        return (RexCall) rexBuilder.makeCall(MULTI_MATCH_FUNCTION, fieldMap, queryMap);
    }
}
