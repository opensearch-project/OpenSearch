/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.query;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.index.query.RegexpQueryBuilder;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RegexpQueryTranslatorTests extends OpenSearchTestCase {

    public void testConvertBasicRegexp() throws Exception {
        RegexpQueryTranslator translator = new RegexpQueryTranslator();
        RegexpQueryBuilder query = new RegexpQueryBuilder("field", ".*pattern.*");

        ConversionContext ctx = mockContext();
        RexNode result = translator.convert(query, ctx);

        assertNotNull(result);
        assertTrue(result instanceof UnresolvedQueryCall);
        UnresolvedQueryCall call = (UnresolvedQueryCall) result;
        assertEquals(query, call.getQueryBuilder());
    }

    public void testConvertWithFlags() throws Exception {
        RegexpQueryTranslator translator = new RegexpQueryTranslator();
        RegexpQueryBuilder query = new RegexpQueryBuilder("field", "pattern").flags(1);

        ConversionContext ctx = mockContext();
        RexNode result = translator.convert(query, ctx);

        assertNotNull(result);
        assertTrue(result instanceof UnresolvedQueryCall);
        UnresolvedQueryCall call = (UnresolvedQueryCall) result;
        assertEquals(query, call.getQueryBuilder());
        assertEquals(1, ((RegexpQueryBuilder) call.getQueryBuilder()).flags());
    }

    public void testGetQueryType() {
        RegexpQueryTranslator translator = new RegexpQueryTranslator();
        assertEquals(RegexpQueryBuilder.class, translator.getQueryType());
    }

    private ConversionContext mockContext() {
        ConversionContext ctx = mock(ConversionContext.class);
        RelOptCluster cluster = mock(RelOptCluster.class);
        RelDataTypeFactory typeFactory = mock(RelDataTypeFactory.class);
        RelDataType boolType = mock(RelDataType.class);

        when(ctx.getCluster()).thenReturn(cluster);
        when(cluster.getTypeFactory()).thenReturn(typeFactory);
        when(typeFactory.createSqlType(SqlTypeName.BOOLEAN)).thenReturn(boolType);

        return ctx;
    }
}
