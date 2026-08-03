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
}
