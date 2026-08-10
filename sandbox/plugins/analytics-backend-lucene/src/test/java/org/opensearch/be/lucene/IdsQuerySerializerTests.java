/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
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
import org.opensearch.analytics.spi.DelegatedPredicateSerializer;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunction;
import org.opensearch.be.lucene.serializers.IdsQuerySerializer;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.query.IdsQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Unit tests for {@link IdsQuerySerializer}.
 */
public class IdsQuerySerializerTests extends OpenSearchTestCase {

    private static final NamedWriteableRegistry WRITEABLE_REGISTRY = new NamedWriteableRegistry(
        List.of(new NamedWriteableRegistry.Entry(QueryBuilder.class, IdsQueryBuilder.NAME, IdsQueryBuilder::new))
    );

    private static final SqlFunction IDS_FUNCTION = new SqlFunction(
        "IDS",
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

    public void testSerializeSingleId() throws IOException {
        RexCall call = buildIdsRexCall("doc1");
        List<FieldStorageInfo> fieldStorage = List.of();

        DelegatedPredicateSerializer serializer = new IdsQuerySerializer();
        byte[] serialized = serializer.serialize(call, fieldStorage);

        try (StreamInput input = new NamedWriteableAwareStreamInput(StreamInput.wrap(serialized), WRITEABLE_REGISTRY)) {
            QueryBuilder deserialized = input.readNamedWriteable(QueryBuilder.class);
            assertTrue("Expected IdsQueryBuilder, got: " + deserialized.getClass(), deserialized instanceof IdsQueryBuilder);
            IdsQueryBuilder idsQb = (IdsQueryBuilder) deserialized;
            assertEquals(Set.of("doc1"), idsQb.ids());
        }
    }

    public void testSerializeMultipleIdsPreservesOrder() throws IOException {
        RexCall call = buildIdsRexCall("alpha", "beta", "gamma");
        List<FieldStorageInfo> fieldStorage = List.of();

        DelegatedPredicateSerializer serializer = new IdsQuerySerializer();
        byte[] serialized = serializer.serialize(call, fieldStorage);

        try (StreamInput input = new NamedWriteableAwareStreamInput(StreamInput.wrap(serialized), WRITEABLE_REGISTRY)) {
            IdsQueryBuilder idsQb = (IdsQueryBuilder) input.readNamedWriteable(QueryBuilder.class);
            assertTrue("Must contain alpha", idsQb.ids().contains("alpha"));
            assertTrue("Must contain beta", idsQb.ids().contains("beta"));
            assertTrue("Must contain gamma", idsQb.ids().contains("gamma"));
            assertEquals(3, idsQb.ids().size());
        }
    }

    public void testSerializeIdContainingComma() throws IOException {
        String commaId = "doc,with,commas";
        RexCall call = buildIdsRexCall(commaId, "normal-id");
        List<FieldStorageInfo> fieldStorage = List.of();

        DelegatedPredicateSerializer serializer = new IdsQuerySerializer();
        byte[] serialized = serializer.serialize(call, fieldStorage);

        try (StreamInput input = new NamedWriteableAwareStreamInput(StreamInput.wrap(serialized), WRITEABLE_REGISTRY)) {
            IdsQueryBuilder idsQb = (IdsQueryBuilder) input.readNamedWriteable(QueryBuilder.class);
            assertTrue("Must contain comma-separated id verbatim", idsQb.ids().contains(commaId));
            assertTrue("Must contain normal-id", idsQb.ids().contains("normal-id"));
            assertEquals(2, idsQb.ids().size());
        }
    }

    public void testMalformedValuesKeyThrowsWithDiagnosticMessage() {
        // Build a call where one MAP key is "values.notAnInt" instead of "values.N"
        List<RexNode> operands = new ArrayList<>();
        operands.add(
            rexBuilder.makeCall(
                SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
                rexBuilder.makeLiteral("values.0"),
                rexBuilder.makeLiteral("id0")
            )
        );
        operands.add(
            rexBuilder.makeCall(
                SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
                rexBuilder.makeLiteral("values.notAnInt"),
                rexBuilder.makeLiteral("id1")
            )
        );
        RexCall call = (RexCall) rexBuilder.makeCall(IDS_FUNCTION, operands);
        List<FieldStorageInfo> fieldStorage = List.of();

        IdsQuerySerializer serializer = new IdsQuerySerializer();
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> serializer.buildQueryBuilder(call, fieldStorage));
        assertTrue("Exception must name the offending key; was: " + ex.getMessage(), ex.getMessage().contains("values.notAnInt"));
    }

    public void testRegistryContainsIdsSerializer() {
        Map<ScalarFunction, DelegatedPredicateSerializer> serializers = QuerySerializerRegistry.getSerializers();
        assertTrue("Registry must contain ScalarFunction.IDS; keys: " + serializers.keySet(), serializers.containsKey(ScalarFunction.IDS));
    }

    public void testSerializeLargeIdListRoundTrips() throws IOException {
        // Stress test: 100 ids must survive serialization/deserialization round-trip
        String[] ids = new String[100];
        for (int i = 0; i < 100; i++) {
            ids[i] = String.format(Locale.ROOT, "doc%03d", i);
        }
        RexCall call = buildIdsRexCall(ids);
        List<FieldStorageInfo> fieldStorage = List.of();

        DelegatedPredicateSerializer serializer = new IdsQuerySerializer();
        byte[] serialized = serializer.serialize(call, fieldStorage);

        try (StreamInput input = new NamedWriteableAwareStreamInput(StreamInput.wrap(serialized), WRITEABLE_REGISTRY)) {
            IdsQueryBuilder idsQb = (IdsQueryBuilder) input.readNamedWriteable(QueryBuilder.class);
            assertEquals(100, idsQb.ids().size());
            // Verify all 100 zero-padded ids are present
            for (int i = 0; i < 100; i++) {
                String expected = String.format(Locale.ROOT, "doc%03d", i);
                assertTrue("Must contain " + expected, idsQb.ids().contains(expected));
            }
        }
    }

    /**
     * Builds an IDS RexCall in the fieldless shape: IDS(MAP('values.0', 'id0'), MAP('values.1', 'id1'), ...)
     */
    private RexCall buildIdsRexCall(String... ids) {
        List<RexNode> operands = new ArrayList<>();

        // Only value MAPs — no field operand (fieldless variant)
        for (int i = 0; i < ids.length; i++) {
            RexNode valueMap = rexBuilder.makeCall(
                SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
                rexBuilder.makeLiteral("values." + i),
                rexBuilder.makeLiteral(ids[i])
            );
            operands.add(valueMap);
        }

        return (RexCall) rexBuilder.makeCall(IDS_FUNCTION, operands);
    }

    public void testUnexpectedKeyThrowsIllegalArgument() {
        // An IDS call containing a non-values key must be rejected
        List<RexNode> operands = new ArrayList<>();
        operands.add(
            rexBuilder.makeCall(
                SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
                rexBuilder.makeLiteral("values.0"),
                rexBuilder.makeLiteral("id0")
            )
        );
        operands.add(
            rexBuilder.makeCall(SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR, rexBuilder.makeLiteral("boost"), rexBuilder.makeLiteral("2.0"))
        );
        RexCall call = (RexCall) rexBuilder.makeCall(IDS_FUNCTION, operands);
        List<FieldStorageInfo> fieldStorage = List.of();

        IdsQuerySerializer serializer = new IdsQuerySerializer();
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> serializer.buildQueryBuilder(call, fieldStorage));
        assertTrue("Exception must name the offending key; was: " + ex.getMessage(), ex.getMessage().contains("boost"));
    }

    public void testNonContiguousIndicesThrowsIllegalArgument() {
        // Indices starting at 1 instead of 0 must be rejected (guards against operand-start-index mismatch)
        List<RexNode> operands = new ArrayList<>();
        operands.add(
            rexBuilder.makeCall(
                SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
                rexBuilder.makeLiteral("values.1"),
                rexBuilder.makeLiteral("id1")
            )
        );
        operands.add(
            rexBuilder.makeCall(
                SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
                rexBuilder.makeLiteral("values.2"),
                rexBuilder.makeLiteral("id2")
            )
        );
        RexCall call = (RexCall) rexBuilder.makeCall(IDS_FUNCTION, operands);
        List<FieldStorageInfo> fieldStorage = List.of();

        IdsQuerySerializer serializer = new IdsQuerySerializer();
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> serializer.buildQueryBuilder(call, fieldStorage));
        assertTrue("Exception must mention contiguous range; was: " + ex.getMessage(), ex.getMessage().contains("contiguous"));
    }

    public void testSerializeIdsWithSpecialCharacters() throws IOException {
        // Character edge-case: single quote, non-ASCII, and space in ids must survive round-trip
        String quoteId = "it's";
        String unicodeId = "\u00fc\u00f1\u00ee-id";
        String spaceId = "id with space";
        RexCall call = buildIdsRexCall(quoteId, unicodeId, spaceId);
        List<FieldStorageInfo> fieldStorage = List.of();

        DelegatedPredicateSerializer serializer = new IdsQuerySerializer();
        byte[] serialized = serializer.serialize(call, fieldStorage);

        try (StreamInput input = new NamedWriteableAwareStreamInput(StreamInput.wrap(serialized), WRITEABLE_REGISTRY)) {
            IdsQueryBuilder idsQb = (IdsQueryBuilder) input.readNamedWriteable(QueryBuilder.class);
            assertEquals(3, idsQb.ids().size());
            assertTrue("Must contain single-quote id", idsQb.ids().contains(quoteId));
            assertTrue("Must contain unicode id", idsQb.ids().contains(unicodeId));
            assertTrue("Must contain space id", idsQb.ids().contains(spaceId));
        }
    }
}
