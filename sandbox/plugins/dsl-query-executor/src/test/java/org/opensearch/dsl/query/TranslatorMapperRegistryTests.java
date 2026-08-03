/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.query;

import org.apache.calcite.rel.type.RelDataTypeField;
import org.opensearch.dsl.TestUtils;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.test.OpenSearchTestCase;

import static org.hamcrest.Matchers.instanceOf;

/**
 * Tests that the translator mapper registry resolves to {@link DefaultTranslatorMapper} for
 * standard Calcite types that have no dedicated mapper registered in this initial step.
 */
public class TranslatorMapperRegistryTests extends OpenSearchTestCase {

    private final ConversionContext ctx = TestUtils.createContext();
    private final TranslatorMapperRegistry registry = TranslatorMapperRegistry.INSTANCE;

    public void testResolveVarcharReturnsDefault() {
        RelDataTypeField field = ctx.getRowType().getField("name", false, false);
        assertThat(registry.resolve(field.getType()), instanceOf(DefaultTranslatorMapper.class));
    }

    public void testResolveIntegerReturnsDefault() {
        RelDataTypeField field = ctx.getRowType().getField("price", false, false);
        assertThat(registry.resolve(field.getType()), instanceOf(DefaultTranslatorMapper.class));
    }

    public void testResolveDoubleReturnsDefault() {
        RelDataTypeField field = ctx.getRowType().getField("rating", false, false);
        assertThat(registry.resolve(field.getType()), instanceOf(DefaultTranslatorMapper.class));
    }

    public void testResolveBigintReturnsDefault() {
        RelDataTypeField field = ctx.getRowType().getField("timestamp", false, false);
        assertThat(registry.resolve(field.getType()), instanceOf(DefaultTranslatorMapper.class));
    }

    public void testResolveTimestampReturnsDefault() {
        RelDataTypeField field = ctx.getRowType().getField("event_time", false, false);
        assertThat(registry.resolve(field.getType()), instanceOf(DefaultTranslatorMapper.class));
    }

    /**
     * Verifies ScaledFloatType resolves to ScaledFloatTranslatorMapper via tier 1.
     */
    public void testResolveScaledFloatReturnsScaledFloatMapper() {
        RelDataTypeField field = ctx.getRowType().getField("scaled_price", false, false);
        assertThat(registry.resolve(field.getType()), instanceOf(ScaledFloatTranslatorMapper.class));
        assertFalse(
            "ScaledFloatType must NOT fall through to DefaultTranslatorMapper",
            registry.resolve(field.getType()) instanceof DefaultTranslatorMapper
        );
    }

    /**
     * Verifies UnsignedLongType resolves to UnsignedLongTranslatorMapper via tier 1.
     */
    public void testResolveUnsignedLongReturnsUnsignedLongMapper() {
        RelDataTypeField field = ctx.getRowType().getField("unsigned_counter", false, false);
        assertThat(registry.resolve(field.getType()), instanceOf(UnsignedLongTranslatorMapper.class));
        assertFalse(
            "UnsignedLongType must NOT fall through to DefaultTranslatorMapper",
            registry.resolve(field.getType()) instanceof DefaultTranslatorMapper
        );
    }

    /**
     * Critical assertion: ScaledFloatType, UnsignedLongType and a plain BIGINT all report
     * SqlTypeName.BIGINT, so this test proves dispatch is keyed on the Java class (tier 1)
     * and not on SqlTypeName. A plain BIGINT field must still resolve to DefaultTranslatorMapper
     * because it has no tier-1 entry.
     */
    public void testPlainBigintResolvesToDefaultNotUdtMapper() {
        // "timestamp" is plain BIGINT in the shared schema, not a UDT subclass
        RelDataTypeField bigintField = ctx.getRowType().getField("timestamp", false, false);
        assertThat(registry.resolve(bigintField.getType()), instanceOf(DefaultTranslatorMapper.class));

        // Verify all three share BIGINT SqlTypeName
        RelDataTypeField scaledField = ctx.getRowType().getField("scaled_price", false, false);
        RelDataTypeField unsignedField = ctx.getRowType().getField("unsigned_counter", false, false);
        assertEquals(bigintField.getType().getSqlTypeName(), scaledField.getType().getSqlTypeName());
        assertEquals(bigintField.getType().getSqlTypeName(), unsignedField.getType().getSqlTypeName());
    }
}
