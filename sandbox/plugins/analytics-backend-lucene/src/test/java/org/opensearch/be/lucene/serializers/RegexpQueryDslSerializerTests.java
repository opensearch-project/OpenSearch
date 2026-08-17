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
import org.opensearch.index.query.RegexpFlag;
import org.opensearch.index.query.RegexpQueryBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RegexpQueryDslSerializerTests extends OpenSearchTestCase {

    private static final NamedWriteableRegistry WRITEABLE_REGISTRY = new NamedWriteableRegistry(
        List.of(new NamedWriteableRegistry.Entry(QueryBuilder.class, RegexpQueryBuilder.NAME, RegexpQueryBuilder::new))
    );

    private static final SqlFunction REGEXP_QUERY_FUNCTION = new SqlFunction(
        "REGEXP_QUERY",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BOOLEAN,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    private static final List<FieldStorageInfo> FIELD_STORAGE = List.of(
        new FieldStorageInfo("title", "keyword", FieldType.KEYWORD, List.of(), List.of("lucene"), List.of(), false)
    );

    private RegexpQueryDslSerializer serializer;
    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        serializer = new RegexpQueryDslSerializer();
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
    }

    public void testBasicRoundTrip() throws IOException {
        RexCall call = buildRexCall("title", "test.*pattern", Map.of());
        byte[] serialized = serializer.serialize(call, FIELD_STORAGE);

        try (StreamInput input = new NamedWriteableAwareStreamInput(StreamInput.wrap(serialized), WRITEABLE_REGISTRY)) {
            QueryBuilder deserialized = input.readNamedWriteable(QueryBuilder.class);
            assertTrue(deserialized instanceof RegexpQueryBuilder);
            RegexpQueryBuilder regexpQb = (RegexpQueryBuilder) deserialized;
            assertEquals("title", regexpQb.fieldName());
            assertEquals("test.*pattern", regexpQb.value());
        }
    }

    public void testCaseInsensitiveRoundTrip() throws IOException {
        RexCall call = buildRexCall("title", "hello", Map.of("case_insensitive", "true"));
        byte[] serialized = serializer.serialize(call, FIELD_STORAGE);

        try (StreamInput input = new NamedWriteableAwareStreamInput(StreamInput.wrap(serialized), WRITEABLE_REGISTRY)) {
            RegexpQueryBuilder regexpQb = (RegexpQueryBuilder) input.readNamedWriteable(QueryBuilder.class);
            assertEquals("title", regexpQb.fieldName());
            assertEquals("hello", regexpQb.value());
            assertTrue("case_insensitive must be true", regexpQb.caseInsensitive());
        }
    }

    public void testFlagsRoundTrip() throws IOException {
        int flagsValue = RegexpFlag.COMPLEMENT.value() | RegexpFlag.INTERSECTION.value();
        RexCall call = buildRexCall("title", "hello", Map.of("flags", String.valueOf(flagsValue)));
        byte[] serialized = serializer.serialize(call, FIELD_STORAGE);

        try (StreamInput input = new NamedWriteableAwareStreamInput(StreamInput.wrap(serialized), WRITEABLE_REGISTRY)) {
            RegexpQueryBuilder regexpQb = (RegexpQueryBuilder) input.readNamedWriteable(QueryBuilder.class);
            assertEquals("title", regexpQb.fieldName());
            assertEquals(flagsValue, regexpQb.flags());
        }
    }

    public void testFlagsNoneRoundTrip() throws IOException {
        // NONE (0) previously broke the name-based round-trip; raw int is lossless.
        RexCall call = buildRexCall("title", "hello", Map.of("flags", "0"));
        byte[] serialized = serializer.serialize(call, FIELD_STORAGE);

        try (StreamInput input = new NamedWriteableAwareStreamInput(StreamInput.wrap(serialized), WRITEABLE_REGISTRY)) {
            RegexpQueryBuilder regexpQb = (RegexpQueryBuilder) input.readNamedWriteable(QueryBuilder.class);
            assertEquals(0, regexpQb.flags());
        }
    }

    public void testMaxDeterminizedStatesRoundTrip() throws IOException {
        RexCall call = buildRexCall("title", "hello", Map.of("max_determinized_states", "20000"));
        byte[] serialized = serializer.serialize(call, FIELD_STORAGE);

        try (StreamInput input = new NamedWriteableAwareStreamInput(StreamInput.wrap(serialized), WRITEABLE_REGISTRY)) {
            RegexpQueryBuilder regexpQb = (RegexpQueryBuilder) input.readNamedWriteable(QueryBuilder.class);
            assertEquals("title", regexpQb.fieldName());
            assertEquals(20000, regexpQb.maxDeterminizedStates());
        }
    }

    public void testRewriteRoundTrip() throws IOException {
        RexCall call = buildRexCall("title", "hello", Map.of("rewrite", "scoring_boolean"));
        byte[] serialized = serializer.serialize(call, FIELD_STORAGE);

        try (StreamInput input = new NamedWriteableAwareStreamInput(StreamInput.wrap(serialized), WRITEABLE_REGISTRY)) {
            RegexpQueryBuilder regexpQb = (RegexpQueryBuilder) input.readNamedWriteable(QueryBuilder.class);
            assertEquals("title", regexpQb.fieldName());
            assertEquals("scoring_boolean", regexpQb.rewrite());
        }
    }

    public void testAllParamsCombinedRoundTrip() throws IOException {
        RexCall call = buildRexCall(
            "title",
            "hello",
            Map.of(
                "case_insensitive",
                "true",
                "flags",
                String.valueOf(RegexpFlag.COMPLEMENT.value()),
                "max_determinized_states",
                "50000",
                "rewrite",
                "constant_score_boolean"
            )
        );
        byte[] serialized = serializer.serialize(call, FIELD_STORAGE);

        try (StreamInput input = new NamedWriteableAwareStreamInput(StreamInput.wrap(serialized), WRITEABLE_REGISTRY)) {
            RegexpQueryBuilder regexpQb = (RegexpQueryBuilder) input.readNamedWriteable(QueryBuilder.class);
            assertEquals("title", regexpQb.fieldName());
            assertEquals("hello", regexpQb.value());
            assertTrue(regexpQb.caseInsensitive());
            assertEquals(RegexpFlag.COMPLEMENT.value(), regexpQb.flags());
            assertEquals(50000, regexpQb.maxDeterminizedStates());
            assertEquals("constant_score_boolean", regexpQb.rewrite());
        }
    }

    // --- Character edge cases: pattern must arrive BYTE-FOR-BYTE unchanged ---

    public void testMalformedBooleanThrows() {
        RexCall call = buildRexCall("title", "test", Map.of("case_insensitive", "yes"));
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> serializer.serialize(call, FIELD_STORAGE));
        assertTrue(ex.getMessage().contains("case_insensitive"));
        assertTrue(ex.getMessage().contains("yes"));
    }

    public void testMalformedIntThrows() {
        RexCall call = buildRexCall("title", "test", Map.of("max_determinized_states", "abc"));
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> serializer.serialize(call, FIELD_STORAGE));
        assertTrue(ex.getMessage().contains("max_determinized_states"));
        assertTrue(ex.getMessage().contains("abc"));
    }

    public void testMalformedFlagsIntThrows() {
        RexCall call = buildRexCall("title", "test", Map.of("flags", "not_a_number"));
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> serializer.serialize(call, FIELD_STORAGE));
        assertTrue(ex.getMessage().contains("flags"));
        assertTrue(ex.getMessage().contains("not_a_number"));
    }

    public void testPatternWithPipe() throws IOException {
        assertPatternRoundTrips("foo|bar");
    }

    public void testPatternWithAmpersand() throws IOException {
        assertPatternRoundTrips("foo&bar");
    }

    public void testPatternWithTilde() throws IOException {
        assertPatternRoundTrips("foo~bar");
    }

    public void testPatternWithInterval() throws IOException {
        assertPatternRoundTrips("value<1-9>");
    }

    public void testPatternWithQuotes() throws IOException {
        assertPatternRoundTrips("\"quoted\"");
    }

    public void testPatternWithBackslash() throws IOException {
        assertPatternRoundTrips("path\\\\to\\\\file");
    }

    public void testPatternWithComma() throws IOException {
        assertPatternRoundTrips("a,b,c");
    }

    public void testPatternWithNonAscii() throws IOException {
        assertPatternRoundTrips("üñî");
    }

    public void testPatternWithCaretAndDollar() throws IOException {
        // In Lucene regexp dialect, ^ and $ are LITERAL characters, NOT anchors
        assertPatternRoundTrips("^start$end");
    }

    // --- Helper methods ---

    private void assertPatternRoundTrips(String pattern) throws IOException {
        RexCall call = buildRexCall("title", pattern, Map.of());
        byte[] serialized = serializer.serialize(call, FIELD_STORAGE);

        try (StreamInput input = new NamedWriteableAwareStreamInput(StreamInput.wrap(serialized), WRITEABLE_REGISTRY)) {
            RegexpQueryBuilder regexpQb = (RegexpQueryBuilder) input.readNamedWriteable(QueryBuilder.class);
            assertEquals("Pattern must arrive byte-for-byte unchanged", pattern, regexpQb.value());
        }
    }

    private RexCall buildRexCall(String fieldName, String pattern, Map<String, String> optionalParams) {
        RelDataType varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR);

        // Operand 0: MAP('field', $0)
        RexNode fieldMap = rexBuilder.makeCall(
            SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
            rexBuilder.makeLiteral("field"),
            rexBuilder.makeInputRef(varcharType, 0)
        );

        // Operand 1: MAP('query', pattern)
        RexNode queryMap = rexBuilder.makeCall(
            SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
            rexBuilder.makeLiteral("query"),
            rexBuilder.makeLiteral(pattern)
        );

        List<RexNode> operands = new ArrayList<>();
        operands.add(fieldMap);
        operands.add(queryMap);

        for (Map.Entry<String, String> entry : optionalParams.entrySet()) {
            RexNode paramMap = rexBuilder.makeCall(
                SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
                rexBuilder.makeLiteral(entry.getKey()),
                rexBuilder.makeLiteral(entry.getValue())
            );
            operands.add(paramMap);
        }

        return (RexCall) rexBuilder.makeCall(REGEXP_QUERY_FUNCTION, operands);
    }
}
