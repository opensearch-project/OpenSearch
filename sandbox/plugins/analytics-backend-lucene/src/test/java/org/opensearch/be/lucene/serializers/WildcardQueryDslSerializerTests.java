/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.serializers;

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
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.WildcardQueryBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Tests for {@link WildcardQueryDslSerializer} — proves Lucene patterns are passed verbatim with no SQL conversion.
 */
public class WildcardQueryDslSerializerTests extends OpenSearchTestCase {

    private static final NamedWriteableRegistry WRITEABLE_REGISTRY = new NamedWriteableRegistry(
        List.of(new NamedWriteableRegistry.Entry(QueryBuilder.class, WildcardQueryBuilder.NAME, WildcardQueryBuilder::new))
    );

    private static final SqlFunction WILDCARD_QUERY_DSL_FUNCTION = new SqlFunction(
        "WILDCARD_QUERY_DSL",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BOOLEAN,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private WildcardQueryDslSerializer serializer;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
        serializer = new WildcardQueryDslSerializer();
    }

    /**
     * Lucene multi-char wildcard: {@code che*k} must arrive unchanged — no conversion to SQL {@code %}.
     */
    public void testLuceneStarPatternPassedVerbatim() throws IOException {
        assertVerbatimPattern("che*k");
    }

    /**
     * Lucene single-char wildcard: {@code ch?ck} must arrive unchanged — no conversion to SQL {@code _}.
     */
    public void testLuceneQuestionMarkPatternPassedVerbatim() throws IOException {
        assertVerbatimPattern("ch?ck");
    }

    /**
     * Escaped literal star: {@code a\*b} must arrive unchanged — the backslash-star is a Lucene escape
     * for a literal star, not an SQL percent.
     */
    public void testEscapedLiteralStarPassedVerbatim() throws IOException {
        assertVerbatimPattern("a\\*b");
    }

    /**
     * Literal backslash in a Windows path: {@code C:\Users} must arrive unchanged.
     */
    public void testLiteralBackslashPassedVerbatim() throws IOException {
        assertVerbatimPattern("C:\\Users");
    }

    /**
     * Pattern containing SQL wildcard characters {@code %} and {@code _} as literals — proves
     * no SQL-to-Lucene conversion occurs (they must NOT become {@code *} and {@code ?}).
     */
    public void testSqlWildcardCharsRemainLiteralNoConversion() throws IOException {
        assertVerbatimPattern("100%_complete");
    }

    /**
     * Verifies case_insensitive param is threaded through to WildcardQueryBuilder.
     */
    public void testCaseInsensitiveParamIsApplied() throws IOException {
        RexCall call = buildWildcardDslCall("title", "che*k", Map.of("case_insensitive", "true"));
        List<FieldStorageInfo> fieldStorage = List.of(
            new FieldStorageInfo("title", "keyword", FieldType.KEYWORD, List.of(), List.of("lucene"), List.of(), false)
        );

        byte[] serialized = serializer.serialize(call, fieldStorage);

        try (StreamInput input = new NamedWriteableAwareStreamInput(StreamInput.wrap(serialized), WRITEABLE_REGISTRY)) {
            WildcardQueryBuilder wildcardQb = (WildcardQueryBuilder) input.readNamedWriteable(QueryBuilder.class);
            assertEquals("title", wildcardQb.fieldName());
            assertEquals("che*k", wildcardQb.value());
            assertTrue("case_insensitive must be true", wildcardQb.caseInsensitive());
        }
    }

    /**
     * Verifies that a malformed operand list (missing 'field') raises IllegalArgumentException
     * with the documented message containing the function name.
     */
    public void testThrowsOnMissingFieldOperand() {
        RexNode queryMap = rexBuilder.makeCall(
            SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
            rexBuilder.makeLiteral("query"),
            rexBuilder.makeLiteral("test*")
        );
        RexCall call = (RexCall) rexBuilder.makeCall(WILDCARD_QUERY_DSL_FUNCTION, queryMap);
        List<FieldStorageInfo> fieldStorage = List.of();

        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> serializer.serialize(call, fieldStorage));
        assertTrue("Message must contain 'wildcard_query_dsl', got: " + ex.getMessage(), ex.getMessage().contains("wildcard_query_dsl"));
    }

    /**
     * Verifies that the rewrite parameter is applied to WildcardQueryBuilder when passed as a MAP operand.
     */
    public void testRewriteParameterAppliedToBuilder() throws IOException {
        RexCall call = buildWildcardDslCall("title", "che*k", Map.of("rewrite", "constant_score"));
        List<FieldStorageInfo> fieldStorage = List.of(
            new FieldStorageInfo("title", "keyword", FieldType.KEYWORD, List.of(), List.of("lucene"), List.of(), false)
        );

        byte[] serialized = serializer.serialize(call, fieldStorage);

        try (StreamInput input = new NamedWriteableAwareStreamInput(StreamInput.wrap(serialized), WRITEABLE_REGISTRY)) {
            WildcardQueryBuilder wildcardQb = (WildcardQueryBuilder) input.readNamedWriteable(QueryBuilder.class);
            assertEquals("title", wildcardQb.fieldName());
            assertEquals("che*k", wildcardQb.value());
            assertEquals("constant_score", wildcardQb.rewrite());
        }
    }

    // ── Helpers ─────────────────────────────────────────────────────────────────

    private void assertVerbatimPattern(String pattern) throws IOException {
        RexCall call = buildWildcardDslCall("title", pattern, Map.of());
        List<FieldStorageInfo> fieldStorage = List.of(
            new FieldStorageInfo("title", "keyword", FieldType.KEYWORD, List.of(), List.of("lucene"), List.of(), false)
        );

        byte[] serialized = serializer.serialize(call, fieldStorage);

        try (StreamInput input = new NamedWriteableAwareStreamInput(StreamInput.wrap(serialized), WRITEABLE_REGISTRY)) {
            QueryBuilder deserialized = input.readNamedWriteable(QueryBuilder.class);
            assertTrue("Must produce WildcardQueryBuilder", deserialized instanceof WildcardQueryBuilder);
            WildcardQueryBuilder wildcardQb = (WildcardQueryBuilder) deserialized;
            assertEquals("title", wildcardQb.fieldName());
            assertEquals("Pattern must be passed verbatim", pattern, wildcardQb.value());
        }
    }

    private RexCall buildWildcardDslCall(String fieldName, String queryText, Map<String, String> params) {
        RelDataType varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR);

        RexNode fieldMap = rexBuilder.makeCall(
            SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
            rexBuilder.makeLiteral("field"),
            rexBuilder.makeInputRef(varcharType, 0)
        );
        RexNode queryMap = rexBuilder.makeCall(
            SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
            rexBuilder.makeLiteral("query"),
            rexBuilder.makeLiteral(queryText)
        );

        List<RexNode> operands = new ArrayList<>();
        operands.add(fieldMap);
        operands.add(queryMap);

        for (Map.Entry<String, String> entry : params.entrySet()) {
            RexNode paramMap = rexBuilder.makeCall(
                SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
                rexBuilder.makeLiteral(entry.getKey()),
                rexBuilder.makeLiteral(entry.getValue())
            );
            operands.add(paramMap);
        }

        return (RexCall) rexBuilder.makeCall(WILDCARD_QUERY_DSL_FUNCTION, operands);
    }
}
