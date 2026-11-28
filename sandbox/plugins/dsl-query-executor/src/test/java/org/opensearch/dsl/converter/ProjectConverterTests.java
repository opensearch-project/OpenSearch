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
import org.apache.calcite.rex.RexNode;
import org.opensearch.dsl.TestUtils;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;

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

    public void testPlaceholderProjectionWhenFetchSourceFalse() throws ConversionException {
        SearchSourceBuilder source = new SearchSourceBuilder().fetchSource(new FetchSourceContext(false, null, null));
        ConversionContext ctx = TestUtils.createContext(source);
        RelNode result = converter.convert(scan, ctx);

        assertTrue(result instanceof LogicalProject);
        assertEquals(List.of(ProjectConverter.EMPTY_SOURCE_PLACEHOLDER), result.getRowType().getFieldNames());
    }

    public void testReturnsUnchangedWhenIncludesEmpty() throws ConversionException {
        SearchSourceBuilder source = new SearchSourceBuilder().fetchSource(new FetchSourceContext(true, new String[] {}, null));
        ConversionContext ctx = TestUtils.createContext(source);
        RelNode result = converter.convert(scan, ctx);

        assertSame(scan, result);
    }

    public void testUnknownFieldUsesEmptySourcePlaceholder() throws ConversionException {
        SearchSourceBuilder source = new SearchSourceBuilder().fetchSource(
            new FetchSourceContext(true, new String[] { "nonexistent" }, null)
        );
        ConversionContext ctx = TestUtils.createContext(source);

        RelNode result = converter.convert(scan, ctx);

        assertEquals(List.of(ProjectConverter.EMPTY_SOURCE_PLACEHOLDER), result.getRowType().getFieldNames());
    }

    public void testUnknownFieldAlongsideKnownFieldIsIgnored() throws ConversionException {
        SearchSourceBuilder source = new SearchSourceBuilder().fetchSource(
            new FetchSourceContext(true, new String[] { "links", "name" }, null)
        );
        ConversionContext ctx = TestUtils.createContext(source);

        RelNode result = converter.convert(scan, ctx);

        assertEquals(List.of("name"), result.getRowType().getFieldNames());
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
        assertEquals(16, result.getRowType().getFieldCount());
        List<String> fieldNames = result.getRowType().getFieldNames();
        assertTrue(fieldNames.contains("name"));
        assertTrue(fieldNames.contains("brand"));
        assertFalse(fieldNames.contains("price"));
        assertFalse(fieldNames.contains("rating"));
    }

    public void testExcludesWithWildcard() throws ConversionException {
        SearchSourceBuilder source = new SearchSourceBuilder().fetchSource(new FetchSourceContext(true, null, new String[] { "ra*" }));
        ConversionContext ctx = TestUtils.createContext(source);
        RelNode result = converter.convert(scan, ctx);

        assertTrue(result instanceof LogicalProject);
        assertEquals(17, result.getRowType().getFieldCount());
        assertFalse(result.getRowType().getFieldNames().contains("rating"));
    }

    public void testWildcardNoMatchUsesEmptySourcePlaceholder() throws ConversionException {
        SearchSourceBuilder source = new SearchSourceBuilder().fetchSource(new FetchSourceContext(true, new String[] { "xyz*" }, null));
        ConversionContext ctx = TestUtils.createContext(source);
        RelNode result = converter.convert(scan, ctx);

        assertTrue(result instanceof LogicalProject);
        assertEquals(List.of(ProjectConverter.EMPTY_SOURCE_PLACEHOLDER), result.getRowType().getFieldNames());
    }

    public void testWildcardIncludesWithExcludes() throws ConversionException {
        // Include all fields matching "*", exclude "rating"
        SearchSourceBuilder source = new SearchSourceBuilder().fetchSource(
            new FetchSourceContext(true, new String[] { "*" }, new String[] { "rating" })
        );
        ConversionContext ctx = TestUtils.createContext(source);
        RelNode result = converter.convert(scan, ctx);

        assertTrue(result instanceof LogicalProject);
        assertEquals(17, result.getRowType().getFieldCount());
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

    public void testObjectIncludeProjectsDescendantFields() throws ConversionException {
        SearchSourceBuilder source = new SearchSourceBuilder().fetchSource(
            new FetchSourceContext(true, new String[] { "attributes" }, null)
        );
        RelNode nestedScan = scanWithFields("attributes.service.name", "attributes.host.name", "body");

        RelNode result = converter.convert(nestedScan, TestUtils.createContext(source));

        assertEquals(List.of("attributes.service.name", "attributes.host.name"), result.getRowType().getFieldNames());
    }

    public void testObjectExcludeRemovesDescendantFields() throws ConversionException {
        SearchSourceBuilder source = new SearchSourceBuilder().fetchSource(
            new FetchSourceContext(true, null, new String[] { "attributes" })
        );
        RelNode nestedScan = scanWithFields("attributes.service.name", "attributes.host.name", "body");

        RelNode result = converter.convert(nestedScan, TestUtils.createContext(source));

        assertEquals(List.of("body"), result.getRowType().getFieldNames());
    }

    private LogicalProject scanWithFields(String... names) {
        List<RexNode> projects = new ArrayList<>();
        for (String ignored : names) {
            projects.add(scan.getCluster().getRexBuilder().makeInputRef(scan, 0));
        }
        return LogicalProject.create(scan, List.of(), projects, List.of(names));
    }
}
