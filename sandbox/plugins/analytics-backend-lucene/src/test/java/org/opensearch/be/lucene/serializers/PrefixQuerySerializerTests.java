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
import org.opensearch.index.query.PrefixQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Tests for {@link PrefixQuerySerializer} — validates verbatim value passthrough and case_insensitive param.
 */
public class PrefixQuerySerializerTests extends OpenSearchTestCase {

    private static final NamedWriteableRegistry WRITEABLE_REGISTRY = new NamedWriteableRegistry(
        List.of(new NamedWriteableRegistry.Entry(QueryBuilder.class, PrefixQueryBuilder.NAME, PrefixQueryBuilder::new))
    );

    private static final SqlFunction PREFIX_QUERY_FUNCTION = new SqlFunction(
        "PREFIX_QUERY",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BOOLEAN,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private PrefixQuerySerializer serializer;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
        serializer = new PrefixQuerySerializer();
    }

    /**
     * Verifies that the serializer passes the prefix value verbatim — including metacharacters that
     * would be special in SQL LIKE or Lucene wildcard context — proving no escaping occurs.
     */
    public void testValuePassedVerbatimIncludingMetacharacters() throws IOException {
        // Value contains *, ? and backslash — must arrive unchanged at PrefixQueryBuilder
        String verbatimValue = "check*it?out\\done";
        RexCall call = buildPrefixCall("title", verbatimValue, Map.of());
        List<FieldStorageInfo> fieldStorage = List.of(
            new FieldStorageInfo("title", "keyword", FieldType.KEYWORD, List.of(), List.of("lucene"), List.of(), false)
        );

        byte[] serialized = serializer.serialize(call, fieldStorage);

        try (StreamInput input = new NamedWriteableAwareStreamInput(StreamInput.wrap(serialized), WRITEABLE_REGISTRY)) {
            QueryBuilder deserialized = input.readNamedWriteable(QueryBuilder.class);
            assertTrue("Must produce PrefixQueryBuilder", deserialized instanceof PrefixQueryBuilder);
            PrefixQueryBuilder prefixQb = (PrefixQueryBuilder) deserialized;
            assertEquals("title", prefixQb.fieldName());
            assertEquals(verbatimValue, prefixQb.value());
        }
    }

    /**
     * Verifies case_insensitive param is threaded through to the PrefixQueryBuilder.
     */
    public void testCaseInsensitiveParamIsApplied() throws IOException {
        RexCall call = buildPrefixCall("name", "lap", Map.of("case_insensitive", "true"));
        List<FieldStorageInfo> fieldStorage = List.of(
            new FieldStorageInfo("name", "keyword", FieldType.KEYWORD, List.of(), List.of("lucene"), List.of(), false)
        );

        byte[] serialized = serializer.serialize(call, fieldStorage);

        try (StreamInput input = new NamedWriteableAwareStreamInput(StreamInput.wrap(serialized), WRITEABLE_REGISTRY)) {
            PrefixQueryBuilder prefixQb = (PrefixQueryBuilder) input.readNamedWriteable(QueryBuilder.class);
            assertEquals("name", prefixQb.fieldName());
            assertEquals("lap", prefixQb.value());
            assertTrue("case_insensitive must be true", prefixQb.caseInsensitive());
        }
    }

    /**
     * Verifies that a malformed operand list (missing 'field') raises IllegalArgumentException
     * with the documented message containing the function name.
     */
    public void testThrowsOnMissingFieldOperand() {
        // Build a call with only a 'query' operand, no 'field'
        RexNode queryMap = rexBuilder.makeCall(
            SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
            rexBuilder.makeLiteral("query"),
            rexBuilder.makeLiteral("test")
        );
        RexCall call = (RexCall) rexBuilder.makeCall(PREFIX_QUERY_FUNCTION, queryMap);
        List<FieldStorageInfo> fieldStorage = List.of();

        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> serializer.serialize(call, fieldStorage));
        assertTrue("Message must contain 'prefix_query', got: " + ex.getMessage(), ex.getMessage().contains("prefix_query"));
    }

    /**
     * Verifies that the rewrite parameter is applied to PrefixQueryBuilder when passed as a MAP operand.
     */
    public void testRewriteParameterAppliedToBuilder() throws IOException {
        RexCall call = buildPrefixCall("title", "lap", Map.of("rewrite", "constant_score"));
        List<FieldStorageInfo> fieldStorage = List.of(
            new FieldStorageInfo("title", "keyword", FieldType.KEYWORD, List.of(), List.of("lucene"), List.of(), false)
        );

        byte[] serialized = serializer.serialize(call, fieldStorage);

        try (StreamInput input = new NamedWriteableAwareStreamInput(StreamInput.wrap(serialized), WRITEABLE_REGISTRY)) {
            PrefixQueryBuilder prefixQb = (PrefixQueryBuilder) input.readNamedWriteable(QueryBuilder.class);
            assertEquals("title", prefixQb.fieldName());
            assertEquals("lap", prefixQb.value());
            assertEquals("constant_score", prefixQb.rewrite());
        }
    }

    // ── Helper ──────────────────────────────────────────────────────────────────

    private RexCall buildPrefixCall(String fieldName, String queryText, Map<String, String> params) {
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

        return (RexCall) rexBuilder.makeCall(PREFIX_QUERY_FUNCTION, operands);
    }
}
