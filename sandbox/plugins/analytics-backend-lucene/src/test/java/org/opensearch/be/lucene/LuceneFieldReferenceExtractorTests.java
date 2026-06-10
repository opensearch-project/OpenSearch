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
import org.opensearch.analytics.spi.FieldReferences;
import org.opensearch.test.OpenSearchTestCase;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * Unit tests for {@link LuceneFieldReferenceExtractor} across the design's Problem-statement cases:
 * explicit MAP fields, in-string fields, ranges, {@code _exists_}, default-field fan-out, pattern
 * classification, lenient resolution, and parse failures.
 */
public class LuceneFieldReferenceExtractorTests extends OpenSearchTestCase {

    private static final SqlFunction RELEVANCE_FUNCTION = new SqlFunction(
        "RELEVANCE_FN",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BOOLEAN,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    private final LuceneFieldReferenceExtractor queryStringExtractor = new LuceneFieldReferenceExtractor("query_string", true);
    private final LuceneFieldReferenceExtractor simpleQueryStringExtractor = new LuceneFieldReferenceExtractor(
        "simple_query_string",
        false
    );

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
    }

    public void testQueryStringExplicitFields() {
        RexCall call = buildCall(List.of("title", "body"), "hello");
        FieldReferences refs = queryStringExtractor.referencedFields(call, List.of());

        assertEquals(List.of("title", "body"), refs.literalFields());
        assertTrue(refs.patternTokens().isEmpty());
        assertFalse("lenient defaults to false when unset (Option B)", refs.lenient());
    }

    public void testQueryStringInStringFieldNoExplicitFields() {
        RexCall call = buildCall(null, "category:A");
        FieldReferences refs = queryStringExtractor.referencedFields(call, List.of());

        assertEquals(List.of("category"), refs.literalFields());
    }

    public void testQueryStringMergesExplicitAndInStringFields() {
        RexCall call = buildCall(List.of("title"), "title:hi AND status:1");
        FieldReferences refs = queryStringExtractor.referencedFields(call, List.of());

        // title (explicit + in-string, deduped) and status (in-string), first-appearance order.
        assertEquals(List.of("title", "status"), refs.literalFields());
    }

    public void testQueryStringPatternTokenNotValidatedAsLiteral() {
        RexCall call = buildCall(List.of("cat*"), "hello");
        FieldReferences refs = queryStringExtractor.referencedFields(call, List.of());

        assertTrue("pattern must not be a literal", refs.literalFields().isEmpty());
        assertEquals(List.of("cat*"), refs.patternTokens());
    }

    public void testQueryStringFieldlessYieldsNoLiterals() {
        RexCall call = buildCall(null, "foo bar");
        FieldReferences refs = queryStringExtractor.referencedFields(call, List.of());

        // Unqualified terms fan out to default_field at execution; nothing for the planner to validate.
        assertTrue(refs.literalFields().isEmpty());
        assertTrue(refs.patternTokens().isEmpty());
    }

    public void testQueryStringExistsField() {
        RexCall call = buildCall(null, "_exists_:status");
        FieldReferences refs = queryStringExtractor.referencedFields(call, List.of());

        assertEquals(List.of("status"), refs.literalFields());
    }

    public void testQueryStringRangeField() {
        RexCall call = buildCall(null, "value:[1 TO 5]");
        FieldReferences refs = queryStringExtractor.referencedFields(call, List.of());

        assertEquals(List.of("value"), refs.literalFields());
    }

    public void testSimpleQueryStringIgnoresInStringSyntax() {
        // simple_query_string has no field:value syntax; only the `fields` operand contributes.
        RexCall call = buildCall(List.of("title", "body"), "category:A");
        FieldReferences refs = simpleQueryStringExtractor.referencedFields(call, List.of());

        assertEquals(List.of("title", "body"), refs.literalFields());
    }

    public void testExplicitLenientHonored() {
        RexCall callFalse = buildCallWithLenient(List.of("intField"), "x", false);
        assertFalse(queryStringExtractor.referencedFields(callFalse, List.of()).lenient());

        RexCall callTrue = buildCallWithLenient(List.of("intField"), "x", true);
        assertTrue(queryStringExtractor.referencedFields(callTrue, List.of()).lenient());

        // Unset → false (Option B): preserves eager rejection.
        RexCall callUnset = buildCall(List.of("intField"), "x");
        assertFalse(queryStringExtractor.referencedFields(callUnset, List.of()).lenient());
    }

    public void testParseFailureFallsBackGracefully() {
        // OpenSearch query_string supports `field:>15`; Lucene's stock parser does not. On parse
        // failure we skip in-string extraction (no throw) and rely on explicit fields.
        RexCall call = buildCall(List.of("severityNumber"), "severityNumber:>15");
        FieldReferences refs = queryStringExtractor.referencedFields(call, List.of());

        assertEquals(List.of("severityNumber"), refs.literalFields());
        assertTrue(refs.patternTokens().isEmpty());
    }

    public void testUnparseableFieldlessQueryFallsBackToFanout() {
        RexCall call = buildCall(null, "title:(unbalanced");
        FieldReferences refs = queryStringExtractor.referencedFields(call, List.of());

        // Parse failed, no explicit fields → nothing for the planner to validate; fan-out resolved at execution.
        assertTrue(refs.literalFields().isEmpty());
        assertTrue(refs.patternTokens().isEmpty());
    }

    // ---- builders ----

    /** Builds {@code fn(MAP('fields', MAP(f1,1.0,...)), MAP('query', queryString))}; omits fields MAP when null. */
    private RexCall buildCall(List<String> fields, String queryString) {
        List<RexNode> operands = new ArrayList<>();
        if (fields != null) {
            operands.add(fieldsOperand(fields));
        }
        operands.add(queryOperand(queryString));
        return (RexCall) rexBuilder.makeCall(RELEVANCE_FUNCTION, operands.toArray(new RexNode[0]));
    }

    /** Builds {@code fn(MAP('fields', ...), MAP('query', ...), MAP('lenient', 'true|false'))}. */
    private RexCall buildCallWithLenient(List<String> fields, String queryString, boolean lenient) {
        RexNode lenientMap = rexBuilder.makeCall(
            SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
            rexBuilder.makeLiteral("lenient"),
            rexBuilder.makeLiteral(Boolean.toString(lenient))
        );
        return (RexCall) rexBuilder.makeCall(RELEVANCE_FUNCTION, fieldsOperand(fields), queryOperand(queryString), lenientMap);
    }

    private RexNode fieldsOperand(List<String> fields) {
        RelDataType doubleType = typeFactory.createSqlType(SqlTypeName.DOUBLE);
        List<RexNode> nested = new ArrayList<>();
        for (String field : fields) {
            nested.add(rexBuilder.makeLiteral(field));
            nested.add(rexBuilder.makeExactLiteral(new BigDecimal("1.0"), doubleType));
        }
        RexNode nestedMap = rexBuilder.makeCall(SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR, nested.toArray(new RexNode[0]));
        return rexBuilder.makeCall(SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR, rexBuilder.makeLiteral("fields"), nestedMap);
    }

    private RexNode queryOperand(String queryString) {
        return rexBuilder.makeCall(
            SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
            rexBuilder.makeLiteral("query"),
            rexBuilder.makeLiteral(queryString)
        );
    }
}
