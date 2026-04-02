/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.opensearch.core.common.text.Text;
import org.opensearch.index.mapper.DocumentMapper;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.Uid;
import org.opensearch.index.query.QueryShardException;
import org.opensearch.search.aggregations.AggregatorTestCase;
import org.opensearch.search.sort.SortBuilders;
import org.opensearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TopMetricsAggregatorTests extends AggregatorTestCase {

    private static final MappedFieldType SORT_FIELD_TYPE = new KeywordFieldMapper.KeywordFieldType("sort_field");
    private static final MappedFieldType METRIC_FIELD_TYPE = new KeywordFieldMapper.KeywordFieldType("metric_field");

    @Override
    protected MapperService mapperServiceMock() {
        MapperService mapperService = mock(MapperService.class);
        DocumentMapper mapper = mock(DocumentMapper.class);
        when(mapper.typeText()).thenReturn(new Text("type"));
        when(mapper.type()).thenReturn("type");
        when(mapperService.documentMapper()).thenReturn(mapper);
        return mapperService;
    }

    public void testTopMetricsOnIndexedDocs() throws Exception {
        TopMetricsAggregationBuilder builder = new TopMetricsAggregationBuilder("tm")
            .metricField("metric_field")
            .sort(SortBuilders.fieldSort("sort_field").order(SortOrder.DESC))
            .size(1);

        InternalTopMetrics result = testCase(new MatchAllDocsQuery(), builder, true);
        assertEquals(List.of("metric_field"), result.getMetricFields());
        assertEquals(1, result.getTopHits().getHits().getHits().length);
        assertEquals("3", result.getTopHits().getHits().getAt(0).getId());
    }

    public void testTopMetricsSortByScore() throws Exception {
        Query query = new QueryParser("sort_field", new KeywordAnalyzer()).parse("c^100 b^10 a^1");
        TopMetricsAggregationBuilder builder = new TopMetricsAggregationBuilder("tm")
            .metricField("metric_field")
            .sort(SortBuilders.scoreSort().order(SortOrder.DESC))
            .size(1);

        InternalTopMetrics result = testCase(query, builder, true);
        assertEquals(1, result.getTopHits().getHits().getHits().length);
        assertEquals("3", result.getTopHits().getHits().getAt(0).getId());
    }

    public void testTopMetricsUnresolvedSortFails() throws Exception {
        TopMetricsAggregationBuilder builder = new TopMetricsAggregationBuilder("tm")
            .metricField("metric_field")
            .sort(SortBuilders.fieldSort("unknown_sort_field").order(SortOrder.DESC))
            .size(1);

        QueryShardException e = expectThrows(QueryShardException.class, () -> testCase(new MatchAllDocsQuery(), builder, false));
        assertTrue(e.getMessage().contains("No mapping found for [unknown_sort_field] in order to sort on"));
    }

    private InternalTopMetrics testCase(Query query, TopMetricsAggregationBuilder builder, boolean includeSortFieldType) throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
                iw.addDocument(document("1", "a", "metric-a"));
                iw.addDocument(document("2", "b", "metric-b"));
                iw.addDocument(document("3", "c", "metric-c"));
            }

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = new IndexSearcher(indexReader);
                if (includeSortFieldType == false) {
                    return searchAndReduce(indexSearcher, query, builder, METRIC_FIELD_TYPE);
                }
                return searchAndReduce(indexSearcher, query, builder, SORT_FIELD_TYPE, METRIC_FIELD_TYPE);
            }
        }
    }

    private Document document(String id, String sortField, String metricField) {
        Document document = new Document();
        document.add(new Field(IdFieldMapper.NAME, Uid.encodeId(id), IdFieldMapper.Defaults.FIELD_TYPE));

        document.add(new Field("sort_field", sortField, KeywordFieldMapper.Defaults.FIELD_TYPE));
        document.add(new SortedSetDocValuesField("sort_field", new BytesRef(sortField)));

        document.add(new Field("metric_field", metricField, KeywordFieldMapper.Defaults.FIELD_TYPE));
        document.add(new SortedSetDocValuesField("metric_field", new BytesRef(metricField)));
        return document;
    }
}
