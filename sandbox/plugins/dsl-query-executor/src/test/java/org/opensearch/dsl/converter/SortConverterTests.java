/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.converter;

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexLiteral;
import org.opensearch.dsl.TestUtils;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.OpenSearchTestCase;

public class SortConverterTests extends OpenSearchTestCase {

    private final SortConverter converter = new SortConverter();
    private final LogicalTableScan scan = TestUtils.createTestRelNode();

    public void testSkipsWhenDefaultPagination() throws ConversionException {
        ConversionContext ctx = TestUtils.createContext(new SearchSourceBuilder());
        RelNode result = converter.convert(scan, ctx);

        assertSame(scan, result);
    }

    public void testSortDescending() throws ConversionException {
        SearchSourceBuilder source = new SearchSourceBuilder().sort("price", SortOrder.DESC);
        ConversionContext ctx = TestUtils.createContext(source);
        RelNode result = converter.convert(scan, ctx);

        assertTrue(result instanceof LogicalSort);
        LogicalSort sort = (LogicalSort) result;

        RelCollation collation = sort.getCollation();
        assertEquals(1, collation.getFieldCollations().size());

        RelFieldCollation fieldCollation = collation.getFieldCollations().get(0);
        assertEquals(1, fieldCollation.getFieldIndex()); // price is index 1
        assertEquals(RelFieldCollation.Direction.DESCENDING, fieldCollation.getDirection());
        assertEquals(RelFieldCollation.NullDirection.FIRST, fieldCollation.nullDirection);
    }

    public void testSortAscending() throws ConversionException {
        SearchSourceBuilder source = new SearchSourceBuilder().sort("name", SortOrder.ASC);
        ConversionContext ctx = TestUtils.createContext(source);
        RelNode result = converter.convert(scan, ctx);

        LogicalSort sort = (LogicalSort) result;
        RelFieldCollation fieldCollation = sort.getCollation().getFieldCollations().get(0);
        assertEquals(0, fieldCollation.getFieldIndex()); // name is index 0
        assertEquals(RelFieldCollation.Direction.ASCENDING, fieldCollation.getDirection());
        assertEquals(RelFieldCollation.NullDirection.LAST, fieldCollation.nullDirection);
    }

    public void testMultipleSortFields() throws ConversionException {
        SearchSourceBuilder source = new SearchSourceBuilder()
            .sort("brand", SortOrder.ASC)
            .sort("price", SortOrder.DESC);
        ConversionContext ctx = TestUtils.createContext(source);
        RelNode result = converter.convert(scan, ctx);

        LogicalSort sort = (LogicalSort) result;
        assertEquals(2, sort.getCollation().getFieldCollations().size());
        assertEquals(2, sort.getCollation().getFieldCollations().get(0).getFieldIndex()); // brand
        assertEquals(1, sort.getCollation().getFieldCollations().get(1).getFieldIndex()); // price
    }

    public void testCustomSize() throws ConversionException {
        SearchSourceBuilder source = new SearchSourceBuilder().size(5);
        ConversionContext ctx = TestUtils.createContext(source);
        RelNode result = converter.convert(scan, ctx);

        LogicalSort sort = (LogicalSort) result;
        assertNull(sort.offset); // from defaults to 0 → no offset
        assertNotNull(sort.fetch);
        assertEquals(5, RexLiteral.intValue(sort.fetch));
    }

    public void testFromAndSize() throws ConversionException {
        SearchSourceBuilder source = new SearchSourceBuilder().from(10).size(5);
        ConversionContext ctx = TestUtils.createContext(source);
        RelNode result = converter.convert(scan, ctx);

        LogicalSort sort = (LogicalSort) result;
        assertNotNull(sort.offset);
        assertNotNull(sort.fetch);
        assertEquals(10, RexLiteral.intValue(sort.offset));
        assertEquals(5, RexLiteral.intValue(sort.fetch));
    }

    public void testThrowsForUnknownSortField() {
        SearchSourceBuilder source = new SearchSourceBuilder().sort("nonexistent", SortOrder.ASC);
        ConversionContext ctx = TestUtils.createContext(source);

        expectThrows(ConversionException.class, () -> converter.convert(scan, ctx));
    }
}
