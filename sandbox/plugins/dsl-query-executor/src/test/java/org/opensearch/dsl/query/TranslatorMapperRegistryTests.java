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
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.dsl.query.range.BoundRequest;
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

    public void testResolveTimestampReturnsTimestampMapper() {
        RelDataTypeField field = ctx.getRowType().getField("event_time", false, false);
        assertThat(registry.resolve(field.getType()), instanceOf(TimestampTranslatorMapper.class));
    }

    /**
     * Verifies a DATE field resolves to TimestampTranslatorMapper via tier 2.
     */
    public void testResolveDateReturnsTimestampMapper() {
        RelDataTypeField field = ctx.getRowType().getField("created_date", false, false);
        assertThat(registry.resolve(field.getType()), instanceOf(TimestampTranslatorMapper.class));
    }

    /**
     * Verifies DateOnlyType resolves to TimestampTranslatorMapper via tier 2.
     * No tier-1 entry is needed because DateOnlyType extends BasicSqlType and reports TIMESTAMP.
     */
    public void testResolveDateOnlyTypeReturnsTimestampMapperViaTier2() {
        RelDataTypeField field = ctx.getRowType().getField("event_nanos", false, false);
        assertThat(registry.resolve(field.getType()), instanceOf(TimestampTranslatorMapper.class));
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

    /**
     * Verifies IpType resolves to IpTranslatorMapper via tier 1.
     * IpType reports SqlTypeName.VARBINARY, but tier 1 keying on exact class takes priority.
     */
    public void testResolveIpTypeReturnsIpMapper() {
        RelDataTypeField field = ctx.getRowType().getField("ip_address", false, false);
        assertThat(registry.resolve(field.getType()), instanceOf(IpTranslatorMapper.class));
        assertFalse(
            "IpType must NOT fall through to RejectingTranslatorMapper",
            registry.resolve(field.getType()) instanceof RejectingTranslatorMapper
        );
    }

    /**
     * Verifies plain VARBINARY (non-IP binary field) resolves to RejectingTranslatorMapper
     * via tier 2, and that its translateBound throws ConversionException.
     */
    public void testResolveVarbinaryReturnsRejectingMapper() {
        RelDataTypeField field = ctx.getRowType().getField("binary_data", false, false);
        assertThat(registry.resolve(field.getType()), instanceOf(RejectingTranslatorMapper.class));

        // Verify translateBound throws ConversionException
        BaseTranslatorMapper mapper = registry.resolve(field.getType());
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> mapper.translateBound(new BoundRequest("abc", true, true, null, null, field, ctx))
        );
        assertTrue("Message should mention field name: " + ex.getMessage(), ex.getMessage().contains("binary_data"));
        assertTrue("Message should mention range queries: " + ex.getMessage(), ex.getMessage().contains("range queries"));
    }
}
