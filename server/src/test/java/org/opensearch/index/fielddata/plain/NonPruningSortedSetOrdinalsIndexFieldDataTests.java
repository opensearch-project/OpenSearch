/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.fielddata.plain;

import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedSetSortField;
import org.opensearch.index.IndexService;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.fielddata.IndexFieldDataService;
import org.opensearch.index.fielddata.plain.NonPruningSortedSetOrdinalsIndexFieldData.NonPruningSortField;
import org.opensearch.index.mapper.ContentPath;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.Mapper.BuilderContext;
import org.opensearch.index.mapper.WildcardFieldMapper;
import org.opensearch.indices.IndicesService;
import org.opensearch.search.MultiValueMode;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.io.IOException;

public class NonPruningSortedSetOrdinalsIndexFieldDataTests extends OpenSearchSingleNodeTestCase {
    public void testNonPruningSortedSetOrdinalsIndexFieldData() throws IOException {
        final IndexService indexService = createIndex("test");
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexFieldDataService ifdService = new IndexFieldDataService(
            indexService.getIndexSettings(),
            indicesService.getIndicesFieldDataCache(),
            indicesService.getCircuitBreakerService(),
            indexService.mapperService(),
            indexService.getThreadPool()
        );
        final BuilderContext ctx = new BuilderContext(indexService.getIndexSettings().getSettings(), new ContentPath(1));
        final MappedFieldType stringMapper = new WildcardFieldMapper.Builder("string").docValues(true).build(ctx).fieldType();
        ifdService.clear();
        IndexFieldData<?> fd = ifdService.getForField(stringMapper, "test", () -> { throw new UnsupportedOperationException(); });
        assertTrue(fd instanceof NonPruningSortedSetOrdinalsIndexFieldData);
        SortField field = ((NonPruningSortedSetOrdinalsIndexFieldData) fd).sortField(null, MultiValueMode.MAX, null, false);
        assertTrue(field instanceof NonPruningSortField);

        // Test all the methods of NonPruningSortField for code coverage

        field.setMissingValue(SortedSetSortField.STRING_FIRST);
        field.getMissingValue();
        field.getField();
        field.getType();
        assertFalse(field.getReverse());
        field.getComparatorSource();
        field.toString();
        field.hashCode();
        field.equals(field);
        field.getBytesComparator();
        field.getComparator(0, Pruning.NONE);
        field.needsScores();
        field.getIndexSorter();
        field.setOptimizeSortWithIndexedData(false);
        field.getOptimizeSortWithIndexedData();
        field.setOptimizeSortWithPoints(false);
        field.getOptimizeSortWithPoints();
    }
}
