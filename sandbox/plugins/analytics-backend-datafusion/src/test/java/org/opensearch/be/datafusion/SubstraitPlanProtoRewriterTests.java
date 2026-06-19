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

    // Builds a nullable PrecisionTime so promotion-focused tests stay no-ops under the
    // nullability-forcing pass (rewriteType forces NULLABILITY_NULLABLE in addition to promoting
    // the type variation). A non-nullable build would be rewritten by the nullability pass alone.
    private static Type makePrecisionTime(int precision, int variationReference) {
        return Type.newBuilder()
            .setPrecisionTime(
                Type.PrecisionTime.newBuilder()
                    .setPrecision(precision)
                    .setTypeVariationReference(variationReference)
                    .setNullability(Type.Nullability.NULLABILITY_NULLABLE)
                    .build()
            )
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
        // A plan whose base_schema is already nullable (makePrecisionTime builds nullable) and has
        // no offending PrecisionTime should round-trip the same instance.
        ReadRel read = ReadRel.newBuilder()
            .setBaseSchema(
                NamedStruct.newBuilder()
                    .addNames("ms")
                    .setStruct(
                        Type.Struct.newBuilder()
                            .setNullability(Type.Nullability.NULLABILITY_NULLABLE)
                            .addTypes(makePrecisionTime(3, 0))
                            .build()
                    )
                    .build()
            )
            .build();
        Plan plan = Plan.newBuilder().addRelations(PlanRel.newBuilder().setRel(Rel.newBuilder().setRead(read).build()).build()).build();
        Plan rewritten = SubstraitPlanProtoRewriter.rewrite(plan);
        assertSame("No-op rewrite returns same instance", plan, rewritten);
    }

    // ── nullability forcing ──────────────────────────────────────────────

    private static Type i64(Type.Nullability nullability) {
        return Type.newBuilder().setI64(Type.I64.newBuilder().setNullability(nullability).build()).build();
    }

    public void testForcesRequiredScalarToNullable() {
        // A NOT NULL i64 (e.g. a count() output) must come back nullable so the DF54 consumer's
        // base_schema-vs-provider nullability check passes.
        Type before = i64(Type.Nullability.NULLABILITY_REQUIRED);
        Type after = SubstraitPlanProtoRewriter.rewriteType(before);
        assertEquals("i64 forced nullable", Type.Nullability.NULLABILITY_NULLABLE, after.getI64().getNullability());
    }

    public void testForcesUnspecifiedScalarToNullable() {
        Type before = i64(Type.Nullability.NULLABILITY_UNSPECIFIED);
        Type after = SubstraitPlanProtoRewriter.rewriteType(before);
        assertEquals("i64 forced nullable", Type.Nullability.NULLABILITY_NULLABLE, after.getI64().getNullability());
    }

    public void testAlreadyNullableScalarReturnsSameInstance() {
        Type before = i64(Type.Nullability.NULLABILITY_NULLABLE);
        Type after = SubstraitPlanProtoRewriter.rewriteType(before);
        assertSame("Already-nullable type is a no-op", before, after);
    }

    public void testForcesNullabilityOnStructAndChildren() {
        // Outer struct REQUIRED, one REQUIRED child and one already-nullable child: every level
        // must end up nullable.
        Type structType = Type.newBuilder()
            .setStruct(
                Type.Struct.newBuilder()
                    .setNullability(Type.Nullability.NULLABILITY_REQUIRED)
                    .addTypes(i64(Type.Nullability.NULLABILITY_REQUIRED))
                    .addTypes(i64(Type.Nullability.NULLABILITY_NULLABLE))
                    .build()
            )
            .build();
        Type after = SubstraitPlanProtoRewriter.rewriteType(structType);
        assertEquals("Struct itself forced nullable", Type.Nullability.NULLABILITY_NULLABLE, after.getStruct().getNullability());
        assertEquals(
            "First child forced nullable",
            Type.Nullability.NULLABILITY_NULLABLE,
            after.getStruct().getTypes(0).getI64().getNullability()
        );
        assertEquals(
            "Second child stays nullable",
            Type.Nullability.NULLABILITY_NULLABLE,
            after.getStruct().getTypes(1).getI64().getNullability()
        );
    }

    public void testForcesNullabilityOnListElement() {
        Type listType = Type.newBuilder()
            .setList(
                Type.List.newBuilder()
                    .setNullability(Type.Nullability.NULLABILITY_REQUIRED)
                    .setType(i64(Type.Nullability.NULLABILITY_REQUIRED))
                    .build()
            )
            .build();
        Type after = SubstraitPlanProtoRewriter.rewriteType(listType);
        assertEquals("List forced nullable", Type.Nullability.NULLABILITY_NULLABLE, after.getList().getNullability());
        assertEquals(
            "List element forced nullable",
            Type.Nullability.NULLABILITY_NULLABLE,
            after.getList().getType().getI64().getNullability()
        );
    }

    public void testForcesNullabilityOnMapKeyAndValue() {
        Type mapType = Type.newBuilder()
            .setMap(
                Type.Map.newBuilder()
                    .setNullability(Type.Nullability.NULLABILITY_REQUIRED)
                    .setKey(i64(Type.Nullability.NULLABILITY_REQUIRED))
                    .setValue(i64(Type.Nullability.NULLABILITY_REQUIRED))
                    .build()
            )
            .build();
        Type after = SubstraitPlanProtoRewriter.rewriteType(mapType);
        assertEquals("Map forced nullable", Type.Nullability.NULLABILITY_NULLABLE, after.getMap().getNullability());
        assertEquals("Map key forced nullable", Type.Nullability.NULLABILITY_NULLABLE, after.getMap().getKey().getI64().getNullability());
        assertEquals(
            "Map value forced nullable",
            Type.Nullability.NULLABILITY_NULLABLE,
            after.getMap().getValue().getI64().getNullability()
        );
    }

    public void testFullPlanForcesNamedTableBaseSchemaNullability() {
        // Mirrors the real regression: a NamedTable base_schema with a NOT NULL aggregate-style
        // column 'c' must be relaxed to nullable through the full rewrite() entry point so the DF54
        // consumer's nullability check against the (nullable) TableProvider schema passes.
        ReadRel read = ReadRel.newBuilder()
            .setNamedTable(ReadRel.NamedTable.newBuilder().addNames("input-1").build())
            .setBaseSchema(
                NamedStruct.newBuilder()
                    .addNames("c")
                    .setStruct(Type.Struct.newBuilder().addTypes(i64(Type.Nullability.NULLABILITY_REQUIRED)).build())
                    .build()
            )
            .build();
        Plan plan = Plan.newBuilder().addRelations(PlanRel.newBuilder().setRel(Rel.newBuilder().setRead(read).build()).build()).build();
        Plan rewritten = SubstraitPlanProtoRewriter.rewrite(plan);
        Type c = rewritten.getRelations(0).getRel().getRead().getBaseSchema().getStruct().getTypes(0);
        assertEquals("base_schema field 'c' forced nullable", Type.Nullability.NULLABILITY_NULLABLE, c.getI64().getNullability());
    }

    public void testVirtualTableBaseSchemaNullabilityPreserved() {
        // A VirtualTable (inline VALUES) read carries row literals that substrait-java's
        // VirtualTableScan.check() asserts must match the base_schema types EXACTLY, including
        // nullability. The rewriter must NOT relax nullability here, or that assertion fails with
        // "Row field type ... does not match schema field type ...". A NOT NULL field must survive.
        // The rewriter routes on read type + base_schema only; the row literals themselves are
        // irrelevant to it, so an empty VirtualTable is sufficient to exercise the skip branch.
        ReadRel read = ReadRel.newBuilder()
            .setVirtualTable(ReadRel.VirtualTable.newBuilder().build())
            .setBaseSchema(
                NamedStruct.newBuilder()
                    .addNames("v")
                    .setStruct(Type.Struct.newBuilder().addTypes(i64(Type.Nullability.NULLABILITY_REQUIRED)).build())
                    .build()
            )
            .build();
        Plan plan = Plan.newBuilder().addRelations(PlanRel.newBuilder().setRel(Rel.newBuilder().setRead(read).build()).build()).build();
        Plan rewritten = SubstraitPlanProtoRewriter.rewrite(plan);
        // No PrecisionTime to promote and nullability forcing is skipped for VirtualTable, so the
        // plan round-trips unchanged and the NOT NULL flag is preserved.
        assertSame("VirtualTable plan with no structural rewrite is a no-op", plan, rewritten);
        Type v = rewritten.getRelations(0).getRel().getRead().getBaseSchema().getStruct().getTypes(0);
        assertEquals("VirtualTable base_schema nullability preserved", Type.Nullability.NULLABILITY_REQUIRED, v.getI64().getNullability());
    }

    public void testVirtualTablePrecisionTimeStillPromoted() {
        // The PrecisionTime promotion is independent of nullability and must still apply to a
        // VirtualTable read, even though nullability is left alone.
        ReadRel read = ReadRel.newBuilder()
            .setVirtualTable(ReadRel.VirtualTable.newBuilder().build())
            .setBaseSchema(
                NamedStruct.newBuilder()
                    .addNames("tm")
                    .setStruct(Type.Struct.newBuilder().addTypes(makePrecisionTime(9, 0)).build())
                    .build()
            )
            .build();
        Plan plan = Plan.newBuilder().addRelations(PlanRel.newBuilder().setRel(Rel.newBuilder().setRead(read).build()).build()).build();
        Plan rewritten = SubstraitPlanProtoRewriter.rewrite(plan);
        Type tm = rewritten.getRelations(0).getRel().getRead().getBaseSchema().getStruct().getTypes(0);
        assertEquals("PrecisionTime promoted even for VirtualTable", 1, tm.getPrecisionTime().getTypeVariationReference());
        assertEquals(
            "VirtualTable PrecisionTime nullability preserved (makePrecisionTime is nullable)",
            Type.Nullability.NULLABILITY_NULLABLE,
            tm.getPrecisionTime().getNullability()
        );
    }
}
