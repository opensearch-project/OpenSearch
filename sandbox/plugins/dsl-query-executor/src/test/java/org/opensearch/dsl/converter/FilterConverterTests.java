/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.converter;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.sql.SqlKind;
import org.opensearch.dsl.TestUtils;
import org.opensearch.dsl.query.QueryRegistryFactory;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

public class FilterConverterTests extends OpenSearchTestCase {

    private final FilterConverter converter = new FilterConverter(QueryRegistryFactory.create());
    private final LogicalTableScan scan = TestUtils.createTestRelNode();

    public void testSkipsWhenNoQuery() throws ConversionException {
        ConversionContext ctx = TestUtils.createContext(new SearchSourceBuilder());
        RelNode result = converter.convert(scan, ctx);

        assertSame(scan, result);
    }

    public void testSkipsMatchAll() throws ConversionException {
        SearchSourceBuilder source = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());
        ConversionContext ctx = TestUtils.createContext(source);
        RelNode result = converter.convert(scan, ctx);

        assertSame(scan, result);
    }

    public void testTermQueryProducesLogicalFilter() throws ConversionException {
        SearchSourceBuilder source = new SearchSourceBuilder().query(QueryBuilders.termQuery("name", "laptop"));
        ConversionContext ctx = TestUtils.createContext(source);
        RelNode result = converter.convert(scan, ctx);

        assertTrue(result instanceof LogicalFilter);
        LogicalFilter filter = (LogicalFilter) result;

        // Input should be the original scan
        assertSame(scan, filter.getInput());

        // Condition should be: name = 'laptop'
        assertTrue(filter.getCondition() instanceof RexCall);
        RexCall call = (RexCall) filter.getCondition();
        assertEquals(SqlKind.EQUALS, call.getKind());

        assertEquals(2, call.getOperands().size());
        // Left operand: field reference to 'name' (index 0)
        assertTrue(call.getOperands().get(0) instanceof RexInputRef);
        assertEquals(0, ((RexInputRef) call.getOperands().get(0)).getIndex());
    }

    public void testFilterPreservesRowType() throws ConversionException {
        SearchSourceBuilder source = new SearchSourceBuilder().query(QueryBuilders.termQuery("brand", "acme"));
        ConversionContext ctx = TestUtils.createContext(source);
        RelNode result = converter.convert(scan, ctx);

        // Filter doesn't change the row type — same fields as the scan
        assertEquals(scan.getRowType(), result.getRowType());
    }

    public void testUnsupportedQueryProducesFilterWithUnresolvedCondition() throws ConversionException {
        SearchSourceBuilder source = new SearchSourceBuilder().query(QueryBuilders.regexpQuery("name", "lap.*"));
        ConversionContext ctx = TestUtils.createContext(source);
        RelNode result = converter.convert(scan, ctx);

        // Unsupported query types produce a LogicalFilter with an UnresolvedQueryCall condition
        assertTrue(result instanceof LogicalFilter);
        LogicalFilter filter = (LogicalFilter) result;
        assertTrue(filter.getCondition() instanceof org.opensearch.dsl.query.UnresolvedQueryCall);
    }
}
