/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.fielddata.plain;

import org.apache.lucene.index.IndexSorter;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedSetSortField;
import org.opensearch.index.IndexService;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.fielddata.IndexFieldDataService;
import org.opensearch.index.mapper.ContentPath;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.Mapper.BuilderContext;
import org.opensearch.index.mapper.WildcardFieldMapper;
import org.opensearch.indices.IndicesService;
import org.opensearch.search.MultiValueMode;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.io.IOException;
import java.util.Comparator;
import java.util.Objects;

public class NonPruningSortedSetOrdinalsIndexFieldDataTests extends OpenSearchSingleNodeTestCase {
    IndexService indexService;
    IndicesService indicesService;
    IndexFieldDataService ifdService;
    BuilderContext ctx;
    MappedFieldType stringMapper;
    SortField field;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        indexService = createIndex("test");
        indicesService = getInstanceFromNode(IndicesService.class);
        ifdService = new IndexFieldDataService(
            indexService.getIndexSettings(),
            indicesService.getIndicesFieldDataCache(),
            indicesService.getCircuitBreakerService(),
            indexService.mapperService(),
            indexService.getThreadPool()
        );
        ctx = new BuilderContext(indexService.getIndexSettings().getSettings(), new ContentPath(1));
        stringMapper = new WildcardFieldMapper.Builder("string").docValues(true).build(ctx).fieldType();
        ifdService.clear();
        IndexFieldData<?> fd = ifdService.getForField(stringMapper, "test", () -> { throw new UnsupportedOperationException(); });
        field = ((NonPruningSortedSetOrdinalsIndexFieldData) fd).sortField(null, MultiValueMode.MAX, null, false);
        field.setMissingValue(SortedSetSortField.STRING_FIRST);
        field.setOptimizeSortWithIndexedData(false);
        field.setOptimizeSortWithPoints(false);
    }

    public void testNonPruningSortedSetOrdinalsIndexFieldDataSerialization() throws IOException {
        assertEquals("<sortedset: \"string\"> missingValue=SortField.STRING_FIRST selector=MAX", field.toString());
    }

    public void testNonPruningSortedSetOrdinalsIndexFieldDataComparator() throws IOException {
        assertTrue(field.getBytesComparator().equals(Comparator.naturalOrder()));
        assertTrue(field.getComparator(0, Pruning.NONE) instanceof FieldComparator);
        assertTrue(field.getIndexSorter() instanceof IndexSorter);
    }

    public void testNonPruningSortedSetOrdinalsIndexFieldDataSorting() throws IOException {
        assertFalse(field.getOptimizeSortWithIndexedData());
        assertFalse(field.getOptimizeSortWithPoints());
        assertFalse(field.needsScores());
        assertTrue(field.getIndexSorter() instanceof IndexSorter);
    }

    public void testNonPruningSortedSetOrdinalsIndexFieldDataEquality() throws IOException {
        assertFalse(field.equals(field));
        assertNotEquals(
            Objects.hash(field.getField(), field.getType(), field.getReverse(), field.getComparatorSource(), field.getMissingValue()),
            field.hashCode()
        );
    }
}
