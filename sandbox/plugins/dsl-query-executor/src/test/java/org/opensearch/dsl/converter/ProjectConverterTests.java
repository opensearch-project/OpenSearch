/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.converter;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.opensearch.dsl.TestUtils;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.test.OpenSearchTestCase;

public class ProjectConverterTests extends OpenSearchTestCase {

    private final ProjectConverter converter = new ProjectConverter();
    private final LogicalTableScan scan = TestUtils.createTestRelNode();

    public void testSkipsWhenNoSourceFiltering() throws ConversionException {
        ConversionContext ctx = TestUtils.createContext(new SearchSourceBuilder());
        RelNode result = converter.convert(scan, ctx);

        assertSame(scan, result);
    }

    public void testProjectsSpecificFields() throws ConversionException {
        SearchSourceBuilder source = new SearchSourceBuilder().fetchSource(
            new FetchSourceContext(true, new String[] { "name", "price" }, null)
        );
        ConversionContext ctx = TestUtils.createContext(source);
        RelNode result = converter.convert(scan, ctx);

        assertTrue(result instanceof LogicalProject);
        assertEquals(2, result.getRowType().getFieldCount());
        assertEquals("name", result.getRowType().getFieldNames().get(0));
        assertEquals("price", result.getRowType().getFieldNames().get(1));
    }

    public void testEmptyProjectionWhenFetchSourceFalse() throws ConversionException {
        SearchSourceBuilder source = new SearchSourceBuilder().fetchSource(new FetchSourceContext(false, null, null));
        ConversionContext ctx = TestUtils.createContext(source);
        RelNode result = converter.convert(scan, ctx);

        assertTrue(result instanceof LogicalProject);
        assertEquals(0, result.getRowType().getFieldCount());
    }

    public void testReturnsUnchangedWhenIncludesEmpty() throws ConversionException {
        SearchSourceBuilder source = new SearchSourceBuilder().fetchSource(new FetchSourceContext(true, new String[] {}, null));
        ConversionContext ctx = TestUtils.createContext(source);
        RelNode result = converter.convert(scan, ctx);

        assertSame(scan, result);
    }

    public void testThrowsForUnknownField() {
        SearchSourceBuilder source = new SearchSourceBuilder().fetchSource(
            new FetchSourceContext(true, new String[] { "nonexistent" }, null)
        );
        ConversionContext ctx = TestUtils.createContext(source);

        expectThrows(ConversionException.class, () -> converter.convert(scan, ctx));
    }

    public void testWildcardProjection() throws ConversionException {
        SearchSourceBuilder source = new SearchSourceBuilder().fetchSource(new FetchSourceContext(true, new String[] { "na*" }, null));
        ConversionContext ctx = TestUtils.createContext(source);
        RelNode result = converter.convert(scan, ctx);

        assertTrue(result instanceof LogicalProject);
        assertEquals(1, result.getRowType().getFieldCount());
        assertEquals("name", result.getRowType().getFieldNames().get(0));
    }

    public void testExcludesFields() throws ConversionException {
        SearchSourceBuilder source = new SearchSourceBuilder().fetchSource(
            new FetchSourceContext(true, null, new String[] { "price", "rating" })
        );
        ConversionContext ctx = TestUtils.createContext(source);
        RelNode result = converter.convert(scan, ctx);

        assertTrue(result instanceof LogicalProject);
        assertEquals(2, result.getRowType().getFieldCount());
        assertEquals("name", result.getRowType().getFieldNames().get(0));
        assertEquals("brand", result.getRowType().getFieldNames().get(1));
    }

    public void testExcludesWithWildcard() throws ConversionException {
        SearchSourceBuilder source = new SearchSourceBuilder().fetchSource(new FetchSourceContext(true, null, new String[] { "ra*" }));
        ConversionContext ctx = TestUtils.createContext(source);
        RelNode result = converter.convert(scan, ctx);

        assertTrue(result instanceof LogicalProject);
        assertEquals(3, result.getRowType().getFieldCount());
        assertFalse(result.getRowType().getFieldNames().contains("rating"));
    }

    public void testWildcardNoMatchReturnsEmptyProjection() throws ConversionException {
        SearchSourceBuilder source = new SearchSourceBuilder().fetchSource(new FetchSourceContext(true, new String[] { "xyz*" }, null));
        ConversionContext ctx = TestUtils.createContext(source);
        RelNode result = converter.convert(scan, ctx);

        // Consistent with OpenSearch core — no error, just empty _source
        assertTrue(result instanceof LogicalProject);
        assertEquals(0, result.getRowType().getFieldCount());
    }

    public void testIncludesWithExcludesAppliesPostFilter() throws ConversionException {
        SearchSourceBuilder source = new SearchSourceBuilder().fetchSource(
            new FetchSourceContext(true, new String[] { "name", "price", "brand" }, new String[] { "price" })
        );
        ConversionContext ctx = TestUtils.createContext(source);
        RelNode result = converter.convert(scan, ctx);

        assertTrue(result instanceof LogicalProject);
        assertEquals(2, result.getRowType().getFieldCount());
        assertEquals("name", result.getRowType().getFieldNames().get(0));
        assertEquals("brand", result.getRowType().getFieldNames().get(1));
    }

    public void testWildcardIncludesWithExcludes() throws ConversionException {
        // Include all fields matching "* ", exclude "rating"
        SearchSourceBuilder source = new SearchSourceBuilder().fetchSource(
            new FetchSourceContext(true, new String[] { "*" }, new String[] { "rating" })
        );
        ConversionContext ctx = TestUtils.createContext(source);
        RelNode result = converter.convert(scan, ctx);

        assertTrue(result instanceof LogicalProject);
        assertEquals(3, result.getRowType().getFieldCount());
        assertFalse(result.getRowType().getFieldNames().contains("rating"));
    }

    public void testOverlappingWildcardsDoNotProduceDuplicates() throws ConversionException {
        // Both "n*" and "na*" match "name" — should only appear once
        SearchSourceBuilder source = new SearchSourceBuilder().fetchSource(
            new FetchSourceContext(true, new String[] { "n*", "na*" }, null)
        );
        ConversionContext ctx = TestUtils.createContext(source);
        RelNode result = converter.convert(scan, ctx);

        assertTrue(result instanceof LogicalProject);
        assertEquals(1, result.getRowType().getFieldCount());
        assertEquals("name", result.getRowType().getFieldNames().get(0));
    }

    public void testDuplicateExactFieldsProjectedOnce() throws ConversionException {
        SearchSourceBuilder source = new SearchSourceBuilder().fetchSource(
            new FetchSourceContext(true, new String[] { "name", "name" }, null)
        );
        ConversionContext ctx = TestUtils.createContext(source);
        RelNode result = converter.convert(scan, ctx);

        assertTrue(result instanceof LogicalProject);
        assertEquals(1, result.getRowType().getFieldCount());
        assertEquals("name", result.getRowType().getFieldNames().get(0));
    }
}
