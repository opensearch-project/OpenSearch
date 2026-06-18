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
import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link ConversionUtils}.
 */
public class ConversionUtilsTests extends OpenSearchTestCase {

    private static final SqlFunction MULTI_MATCH_FUNCTION = new SqlFunction(
        "MULTI_MATCH",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BOOLEAN,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    private static final SqlFunction MATCH_FUNCTION = new SqlFunction(
        "MATCH",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BOOLEAN,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    private static final SqlFunction QUERY_STRING_FUNCTION = new SqlFunction(
        "QUERY_STRING",
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
     * Tests that extractFieldsFromRelevanceMap returns a single-element list when the
     * nested MAP operand contains exactly one field name/boost pair.
     * Validates Requirement 9.2.
     */
    public void testSingleFieldExtractionReturnsOneElementList() {
        // Structure: MULTI_MATCH(MAP('fields', MAP('title', 1.0)), MAP('query', 'hello'))
        RelDataType doubleType = typeFactory.createSqlType(SqlTypeName.DOUBLE);

        // Inner nested MAP: field name literal + boost literal (boost ignored, only field name extracted)
        RexNode fieldNameLiteral = rexBuilder.makeLiteral("title");
        RexNode boostLiteral = rexBuilder.makeExactLiteral(new BigDecimal("1.0"), doubleType);
        RexNode nestedMap = rexBuilder.makeCall(SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR, fieldNameLiteral, boostLiteral);

        // Outer MAP: MAP('fields', nestedMap)
        RexNode fieldsKeyLiteral = rexBuilder.makeLiteral("fields");
        RexNode outerMap = rexBuilder.makeCall(SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR, fieldsKeyLiteral, nestedMap);

        // Query MAP: MAP('query', 'hello')
        RexNode queryMap = rexBuilder.makeCall(
            SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
            rexBuilder.makeLiteral("query"),
            rexBuilder.makeLiteral("hello")
        );

        // Top-level call: MULTI_MATCH(outerMap, queryMap)
        RexCall topCall = (RexCall) rexBuilder.makeCall(MULTI_MATCH_FUNCTION, outerMap, queryMap);

        // FieldStorageInfo list is not used in the nested MAP path (literal field names)
        List<FieldStorageInfo> fieldStorage = List.of();

        List<String> result = ConversionUtils.extractFieldsFromRelevanceMap(topCall, 0, fieldStorage);

        assertEquals("Should return exactly one field", 1, result.size());
        assertEquals("title", result.get(0));
    }

    /**
     * Tests that extractFieldsFromRelevanceMap returns all fields in order when the
     * nested MAP operand contains multiple field name/boost pairs.
     * Validates Requirement 9.1.
     */
    public void testMultiFieldExtractionReturnsAllFieldsInOrder() {
        // Structure: MAP('fields', MAP('title', 2.0, 'body', 1.0, 'tags', 0.5))
        RelDataType doubleType = typeFactory.createSqlType(SqlTypeName.DOUBLE);

        RexNode titleLiteral = rexBuilder.makeLiteral("title");
        RexNode titleBoost = rexBuilder.makeExactLiteral(new BigDecimal("2.0"), doubleType);
        RexNode bodyLiteral = rexBuilder.makeLiteral("body");
        RexNode bodyBoost = rexBuilder.makeExactLiteral(new BigDecimal("1.0"), doubleType);
        RexNode tagsLiteral = rexBuilder.makeLiteral("tags");
        RexNode tagsBoost = rexBuilder.makeExactLiteral(new BigDecimal("0.5"), doubleType);

        // Nested MAP with 3 field/boost pairs
        RexNode nestedMap = rexBuilder.makeCall(
            SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
            titleLiteral,
            titleBoost,
            bodyLiteral,
            bodyBoost,
            tagsLiteral,
            tagsBoost
        );

        // Outer MAP: MAP('fields', nestedMap)
        RexNode outerMap = rexBuilder.makeCall(SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR, rexBuilder.makeLiteral("fields"), nestedMap);

        // Query MAP
        RexNode queryMap = rexBuilder.makeCall(
            SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
            rexBuilder.makeLiteral("query"),
            rexBuilder.makeLiteral("search text")
        );

        RexCall topCall = (RexCall) rexBuilder.makeCall(MULTI_MATCH_FUNCTION, outerMap, queryMap);
        List<FieldStorageInfo> fieldStorage = List.of();

        List<String> result = ConversionUtils.extractFieldsFromRelevanceMap(topCall, 0, fieldStorage);

        assertEquals("Should return exactly three fields", 3, result.size());
        assertEquals("title", result.get(0));
        assertEquals("body", result.get(1));
        assertEquals("tags", result.get(2));
    }

    /**
     * Tests that extractFieldsFromRelevanceMap works with the RexInputRef fallback path
     * when the operand uses the MAP('field', $ref1, 'field', $ref2, ...) structure.
     * Validates Requirement 9.1 (fallback path).
     */
    public void testMultiFieldExtractionWithRexInputRefFallback() {
        // Structure: MAP('field', $0, 'field', $1) — RexInputRef-based multi-field
        RelDataType varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR);

        RexNode key1 = rexBuilder.makeLiteral("field");
        RexNode ref0 = rexBuilder.makeInputRef(varcharType, 0);
        RexNode key2 = rexBuilder.makeLiteral("field");
        RexNode ref1 = rexBuilder.makeInputRef(varcharType, 1);

        RexNode fieldMap = rexBuilder.makeCall(SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR, key1, ref0, key2, ref1);

        RexNode queryMap = rexBuilder.makeCall(
            SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
            rexBuilder.makeLiteral("query"),
            rexBuilder.makeLiteral("test query")
        );

        RexCall topCall = (RexCall) rexBuilder.makeCall(MULTI_MATCH_FUNCTION, fieldMap, queryMap);

        // FieldStorageInfo list maps index 0 → "title", index 1 → "body"
        List<FieldStorageInfo> fieldStorage = List.of(
            new FieldStorageInfo("title", "keyword", FieldType.KEYWORD, List.of(), List.of("lucene"), List.of(), false),
            new FieldStorageInfo("body", "text", FieldType.TEXT, List.of(), List.of("lucene"), List.of(), false)
        );

        List<String> result = ConversionUtils.extractFieldsFromRelevanceMap(topCall, 0, fieldStorage);

        assertEquals("Should return exactly two fields", 2, result.size());
        assertEquals("title", result.get(0));
        assertEquals("body", result.get(1));
    }

    /**
     * Tests that extractFieldsFromRelevanceMap throws IllegalArgumentException when the
     * operand contains no resolvable field references.
     * Validates Requirement 9.3.
     */
    public void testNoFieldsThrowsIllegalArgumentException() {
        // Structure: MAP('fields', 'not_a_map') — value is a literal, not a nested RexCall
        RexNode outerMap = rexBuilder.makeCall(
            SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
            rexBuilder.makeLiteral("fields"),
            rexBuilder.makeLiteral("not_a_map")
        );

        RexNode queryMap = rexBuilder.makeCall(
            SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
            rexBuilder.makeLiteral("query"),
            rexBuilder.makeLiteral("test")
        );

        RexCall topCall = (RexCall) rexBuilder.makeCall(MULTI_MATCH_FUNCTION, outerMap, queryMap);
        List<FieldStorageInfo> fieldStorage = List.of();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> ConversionUtils.extractFieldsFromRelevanceMap(topCall, 0, fieldStorage)
        );
        assertTrue(
            "Exception message should mention operand index",
            exception.getMessage().contains("Cannot extract field names from operand 0")
        );
    }

    // --- Tests for extractMapKey ---

    /**
     * Tests that extractMapKey returns the key string from a MAP_VALUE_CONSTRUCTOR operand.
     * Structure: MATCH(MAP('field', $0), MAP('query', 'hello'))
     * extractMapKey(call, 0) should return "field".
     */
    public void testExtractMapKey_returnsKeyFromMapCall() {
        RelDataType varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR);

        RexNode fieldMap = rexBuilder.makeCall(
            SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
            rexBuilder.makeLiteral("field"),
            rexBuilder.makeInputRef(varcharType, 0)
        );

        RexNode queryMap = rexBuilder.makeCall(
            SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
            rexBuilder.makeLiteral("query"),
            rexBuilder.makeLiteral("hello")
        );

        RexCall topCall = (RexCall) rexBuilder.makeCall(MATCH_FUNCTION, fieldMap, queryMap);

        String key = ConversionUtils.extractMapKey(topCall, 0);
        assertEquals("field", key);
    }

    /**
     * Tests that extractMapKey returns null when the operand is a plain RexInputRef (not a MAP).
     */
    public void testExtractMapKey_returnsNullForNonMapOperand() {
        RelDataType varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR);

        // Direct RexInputRef operand (no MAP wrapper)
        RexNode inputRef = rexBuilder.makeInputRef(varcharType, 0);
        RexNode literal = rexBuilder.makeLiteral("hello");

        RexCall topCall = (RexCall) rexBuilder.makeCall(MATCH_FUNCTION, inputRef, literal);

        // RexInputRef is not a RexCall, so extractMapKey should return null
        assertNull(ConversionUtils.extractMapKey(topCall, 0));
        // RexLiteral is not a RexCall, so extractMapKey should return null
        assertNull(ConversionUtils.extractMapKey(topCall, 1));
    }

    // --- Tests for extractRelevanceOperands ---

    /**
     * Tests that extractRelevanceOperands correctly extracts field and query from MAP-wrapped operands.
     * Structure: MATCH(MAP('field', $0), MAP('query', 'hello'))
     * Should resolve fieldName from fieldStorage and query from the literal.
     */
    public void testExtractRelevanceOperands_mapWrappedFieldAndQuery() {
        RelDataType varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR);

        RexNode fieldMap = rexBuilder.makeCall(
            SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
            rexBuilder.makeLiteral("field"),
            rexBuilder.makeInputRef(varcharType, 0)
        );

        RexNode queryMap = rexBuilder.makeCall(
            SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
            rexBuilder.makeLiteral("query"),
            rexBuilder.makeLiteral("hello")
        );

        RexCall topCall = (RexCall) rexBuilder.makeCall(MATCH_FUNCTION, fieldMap, queryMap);

        List<FieldStorageInfo> fieldStorage = List.of(
            new FieldStorageInfo("status", "keyword", FieldType.KEYWORD, List.of(), List.of("lucene"), List.of(), false)
        );

        ConversionUtils.RelevanceOperands operands = ConversionUtils.extractRelevanceOperands(topCall, fieldStorage);

        assertEquals("status", operands.fieldName());
        assertNull(operands.fields());
        assertEquals("hello", operands.query());
    }

    /**
     * Tests that extractRelevanceOperands handles a query-only MAP structure (no field MAP).
     * Structure: query_string(MAP('query', 'brewing'))
     * Should return fieldName=null, fields=null, query="brewing".
     */
    public void testExtractRelevanceOperands_queryOnlyNoField() {
        RexNode queryMap = rexBuilder.makeCall(
            SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
            rexBuilder.makeLiteral("query"),
            rexBuilder.makeLiteral("brewing")
        );

        RexCall topCall = (RexCall) rexBuilder.makeCall(QUERY_STRING_FUNCTION, queryMap);

        List<FieldStorageInfo> fieldStorage = List.of();

        ConversionUtils.RelevanceOperands operands = ConversionUtils.extractRelevanceOperands(topCall, fieldStorage);

        assertNull(operands.fieldName());
        assertNull(operands.fields());
        assertEquals("brewing", operands.query());
    }

    /**
     * Tests that extractRelevanceOperands falls back to positional extraction when operands
     * are not MAP-wrapped (e.g. MATCH($0, 'hello') with direct RexInputRef and RexLiteral).
     */
    public void testExtractRelevanceOperands_positionalFallback() {
        RelDataType varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR);

        // Direct operands without MAP wrappers
        RexNode inputRef = rexBuilder.makeInputRef(varcharType, 0);
        RexNode literal = rexBuilder.makeLiteral("hello");

        RexCall topCall = (RexCall) rexBuilder.makeCall(MATCH_FUNCTION, inputRef, literal);

        List<FieldStorageInfo> fieldStorage = List.of(
            new FieldStorageInfo("status", "keyword", FieldType.KEYWORD, List.of(), List.of("lucene"), List.of(), false)
        );

        ConversionUtils.RelevanceOperands operands = ConversionUtils.extractRelevanceOperands(topCall, fieldStorage);

        assertEquals("status", operands.fieldName());
        assertNull(operands.fields());
        assertEquals("hello", operands.query());
    }

    // --- Tests for extractOptionalParams ---

    /**
     * Tests that extractOptionalParams extracts multiple key-value pairs from MAP operands.
     * Structure: MATCH(MAP('field', $0), MAP('query', 'hello'), MAP('operator', 'AND'), MAP('analyzer', 'standard'))
     * extractOptionalParams(call, 2) should return {"operator": "AND", "analyzer": "standard"}.
     * Validates Requirement 11.1.
     */
    public void testExtractOptionalParams_multipleKeyValuePairs() {
        RelDataType varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR);

        RexNode fieldMap = rexBuilder.makeCall(
            SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
            rexBuilder.makeLiteral("field"),
            rexBuilder.makeInputRef(varcharType, 0)
        );

        RexNode queryMap = rexBuilder.makeCall(
            SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
            rexBuilder.makeLiteral("query"),
            rexBuilder.makeLiteral("hello")
        );

        RexNode operatorMap = rexBuilder.makeCall(
            SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
            rexBuilder.makeLiteral("operator"),
            rexBuilder.makeLiteral("AND")
        );

        RexNode analyzerMap = rexBuilder.makeCall(
            SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
            rexBuilder.makeLiteral("analyzer"),
            rexBuilder.makeLiteral("standard")
        );

        RexCall topCall = (RexCall) rexBuilder.makeCall(MATCH_FUNCTION, fieldMap, queryMap, operatorMap, analyzerMap);

        Map<String, String> params = ConversionUtils.extractOptionalParams(topCall, 2);

        assertEquals(2, params.size());
        assertEquals("AND", params.get("operator"));
        assertEquals("standard", params.get("analyzer"));
    }

    /**
     * Tests that extractOptionalParams returns an empty map when startIndex exceeds operand count.
     * Validates Requirement 11.9.
     */
    public void testExtractOptionalParams_emptyWhenStartIndexExceedsOperandCount() {
        RelDataType varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR);

        RexNode fieldMap = rexBuilder.makeCall(
            SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
            rexBuilder.makeLiteral("field"),
            rexBuilder.makeInputRef(varcharType, 0)
        );

        RexNode queryMap = rexBuilder.makeCall(
            SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
            rexBuilder.makeLiteral("query"),
            rexBuilder.makeLiteral("hello")
        );

        RexCall topCall = (RexCall) rexBuilder.makeCall(MATCH_FUNCTION, fieldMap, queryMap);

        Map<String, String> params = ConversionUtils.extractOptionalParams(topCall, 2);

        assertTrue("Should return empty map when no optional params", params.isEmpty());
    }

    /**
     * Tests that extractOptionalParams silently skips non-MAP operands (e.g. plain RexLiteral).
     * Validates Requirement 11.10.
     */
    public void testExtractOptionalParams_skipsNonMapOperands() {
        RelDataType varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR);

        RexNode fieldMap = rexBuilder.makeCall(
            SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
            rexBuilder.makeLiteral("field"),
            rexBuilder.makeInputRef(varcharType, 0)
        );

        RexNode queryMap = rexBuilder.makeCall(
            SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
            rexBuilder.makeLiteral("query"),
            rexBuilder.makeLiteral("hello")
        );

        // A valid MAP param
        RexNode operatorMap = rexBuilder.makeCall(
            SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
            rexBuilder.makeLiteral("operator"),
            rexBuilder.makeLiteral("AND")
        );

        // A plain literal (not a MAP) — should be skipped
        RexNode plainLiteral = rexBuilder.makeLiteral("stray_value");

        RexCall topCall = (RexCall) rexBuilder.makeCall(MATCH_FUNCTION, fieldMap, queryMap, operatorMap, plainLiteral);

        Map<String, String> params = ConversionUtils.extractOptionalParams(topCall, 2);

        assertEquals("Should only extract the valid MAP param", 1, params.size());
        assertEquals("AND", params.get("operator"));
    }

    /**
     * Tests that extractOptionalParams silently skips MAP operands with non-literal children.
     * Validates Requirement 11.10.
     */
    public void testExtractOptionalParams_skipsMapWithNonLiteralChildren() {
        RelDataType varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR);

        RexNode fieldMap = rexBuilder.makeCall(
            SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
            rexBuilder.makeLiteral("field"),
            rexBuilder.makeInputRef(varcharType, 0)
        );

        RexNode queryMap = rexBuilder.makeCall(
            SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
            rexBuilder.makeLiteral("query"),
            rexBuilder.makeLiteral("hello")
        );

        // MAP with a RexInputRef as value (not a literal) — should be skipped
        RexNode mapWithRef = rexBuilder.makeCall(
            SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
            rexBuilder.makeLiteral("bad_param"),
            rexBuilder.makeInputRef(varcharType, 0)
        );

        // A valid MAP param
        RexNode validMap = rexBuilder.makeCall(
            SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
            rexBuilder.makeLiteral("analyzer"),
            rexBuilder.makeLiteral("whitespace")
        );

        RexCall topCall = (RexCall) rexBuilder.makeCall(MATCH_FUNCTION, fieldMap, queryMap, mapWithRef, validMap);

        Map<String, String> params = ConversionUtils.extractOptionalParams(topCall, 2);

        assertEquals("Should only extract the valid MAP param", 1, params.size());
        assertEquals("whitespace", params.get("analyzer"));
    }
}
