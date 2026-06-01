/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.test.OpenSearchTestCase;

import io.substrait.proto.NamedStruct;
import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.proto.ReadRel;
import io.substrait.proto.Rel;
import io.substrait.proto.Type;

/**
 * Unit tests for {@link SubstraitPlanProtoRewriter} — verifies that PrecisionTime
 * fields with {@code type_variation_reference == 0} and {@code precision in {6, 9}}
 * are promoted to variation 1 (TIME_64), while other PrecisionTime values pass
 * through unchanged.
 */
public class SubstraitPlanProtoRewriterTests extends OpenSearchTestCase {

    private static Type makePrecisionTime(int precision, int variationReference) {
        return Type.newBuilder()
            .setPrecisionTime(Type.PrecisionTime.newBuilder().setPrecision(precision).setTypeVariationReference(variationReference).build())
            .build();
    }

    public void testPromotesVariation0Precision9() {
        Type before = makePrecisionTime(9, 0);
        Type after = SubstraitPlanProtoRewriter.rewriteType(before);
        assertTrue("Type should still be PrecisionTime", after.hasPrecisionTime());
        assertEquals("Precision preserved", 9, after.getPrecisionTime().getPrecision());
        assertEquals("Variation promoted to 1 (TIME_64)", 1, after.getPrecisionTime().getTypeVariationReference());
    }

    public void testPromotesVariation0Precision6() {
        Type before = makePrecisionTime(6, 0);
        Type after = SubstraitPlanProtoRewriter.rewriteType(before);
        assertEquals(6, after.getPrecisionTime().getPrecision());
        assertEquals("Variation promoted to 1", 1, after.getPrecisionTime().getTypeVariationReference());
    }

    public void testLeavesVariation0Precision3Alone() {
        // Time32(Millisecond) is legal — must not be promoted.
        Type before = makePrecisionTime(3, 0);
        Type after = SubstraitPlanProtoRewriter.rewriteType(before);
        assertSame("No-op when precision is legal for Time32", before, after);
        assertEquals(0, after.getPrecisionTime().getTypeVariationReference());
    }

    public void testLeavesVariation0Precision0Alone() {
        // Time32(Second) is legal — must not be promoted.
        Type before = makePrecisionTime(0, 0);
        Type after = SubstraitPlanProtoRewriter.rewriteType(before);
        assertSame(before, after);
        assertEquals(0, after.getPrecisionTime().getTypeVariationReference());
    }

    public void testLeavesVariation1Alone() {
        // Already explicitly Time64 — must remain as authored.
        Type before = makePrecisionTime(9, 1);
        Type after = SubstraitPlanProtoRewriter.rewriteType(before);
        assertSame(before, after);
        assertEquals(1, after.getPrecisionTime().getTypeVariationReference());
    }

    public void testRecursesIntoStruct() {
        Type structType = Type.newBuilder()
            .setStruct(
                Type.Struct.newBuilder()
                    .addTypes(makePrecisionTime(9, 0)) // should be promoted
                    .addTypes(makePrecisionTime(3, 0)) // should be left alone
                    .build()
            )
            .build();
        Type rewritten = SubstraitPlanProtoRewriter.rewriteType(structType);
        assertNotSame("Struct rewritten because inner type changed", structType, rewritten);
        assertEquals("First inner type promoted", 1, rewritten.getStruct().getTypes(0).getPrecisionTime().getTypeVariationReference());
        assertEquals("Second inner type unchanged", 0, rewritten.getStruct().getTypes(1).getPrecisionTime().getTypeVariationReference());
    }

    public void testRecursesIntoList() {
        Type listType = Type.newBuilder().setList(Type.List.newBuilder().setType(makePrecisionTime(9, 0)).build()).build();
        Type rewritten = SubstraitPlanProtoRewriter.rewriteType(listType);
        assertEquals("List element type promoted", 1, rewritten.getList().getType().getPrecisionTime().getTypeVariationReference());
    }

    public void testRecursesIntoMap() {
        Type mapType = Type.newBuilder()
            .setMap(Type.Map.newBuilder().setKey(makePrecisionTime(9, 0)).setValue(makePrecisionTime(6, 0)).build())
            .build();
        Type rewritten = SubstraitPlanProtoRewriter.rewriteType(mapType);
        assertEquals("Map key promoted", 1, rewritten.getMap().getKey().getPrecisionTime().getTypeVariationReference());
        assertEquals("Map value promoted", 1, rewritten.getMap().getValue().getPrecisionTime().getTypeVariationReference());
    }

    public void testFullPlanRewriteThroughReadRel() {
        // Build a minimal Plan { relations: [Read { base_schema: Struct { types: [PrecisionTime(9, 0)] } }] }
        // and confirm rewrite() promotes the PrecisionTime in the read's base schema.
        ReadRel read = ReadRel.newBuilder()
            .setBaseSchema(
                NamedStruct.newBuilder()
                    .addNames("tm")
                    .setStruct(Type.Struct.newBuilder().addTypes(makePrecisionTime(9, 0)).build())
                    .build()
            )
            .build();
        Rel rel = Rel.newBuilder().setRead(read).build();
        PlanRel planRel = PlanRel.newBuilder().setRel(rel).build();
        Plan plan = Plan.newBuilder().addRelations(planRel).build();

        Plan rewritten = SubstraitPlanProtoRewriter.rewrite(plan);
        Type rewrittenType = rewritten.getRelations(0).getRel().getRead().getBaseSchema().getStruct().getTypes(0);
        assertEquals("PrecisionTime in ReadRel.base_schema promoted", 1, rewrittenType.getPrecisionTime().getTypeVariationReference());
        assertEquals("Precision still 9", 9, rewrittenType.getPrecisionTime().getPrecision());
    }

    public void testNoOpReturnsSameInstance() {
        // A plan with no offending PrecisionTime should round-trip the same instance.
        ReadRel read = ReadRel.newBuilder()
            .setBaseSchema(
                NamedStruct.newBuilder()
                    .addNames("ms")
                    .setStruct(Type.Struct.newBuilder().addTypes(makePrecisionTime(3, 0)).build())
                    .build()
            )
            .build();
        Plan plan = Plan.newBuilder().addRelations(PlanRel.newBuilder().setRel(Rel.newBuilder().setRead(read).build()).build()).build();
        Plan rewritten = SubstraitPlanProtoRewriter.rewrite(plan);
        assertSame("No-op rewrite returns same instance", plan, rewritten);
    }
}
