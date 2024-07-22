package org.opensearch.search.aggregations.bucket.terms;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.DerivedField;
import org.opensearch.index.mapper.DerivedFieldResolver;
import org.opensearch.index.mapper.DerivedFieldResolverFactory;
import org.opensearch.index.mapper.DerivedFieldType;
import org.opensearch.index.mapper.DerivedFieldValueFetcher;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.script.DerivedFieldScript;
import org.opensearch.script.Script;
import org.opensearch.search.aggregations.AggregatorTestCase;
import org.opensearch.search.lookup.LeafSearchLookup;
import org.opensearch.search.lookup.SearchLookup;
import org.opensearch.search.lookup.SourceLookup;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class DerivedFieldAggregationTests extends AggregatorTestCase {

    private QueryShardContext mockContext;
    private List<Document> docs;

    private static final String[][] raw_requests = new String[][] {
        { "40.135.0.0 GET /images/hm_bg.jpg HTTP/1.0", "200", "40.135.0.0" },
        { "232.0.0.0 GET /images/hm_bg.jpg HTTP/1.0", "400", "232.0.0.0" },
        { "26.1.0.0 GET /images/hm_bg.jpg HTTP/1.0", "200", "26.1.0.0" },
        { "247.37.0.0 GET /french/splash_inet.html HTTP/1.0", "400", "247.37.0.0" },
        { "247.37.0.0 GET /french/splash_inet.html HTTP/1.0", "400", "247.37.0.0" },
        { "247.37.0.0 GET /french/splash_inet.html HTTP/1.0", "200", "247.37.0.0" } };

    @Before
    public void init() {
        super.initValuesSourceRegistry();
        // Create a mock QueryShardContext
        mockContext = mock(QueryShardContext.class);
        when(mockContext.index()).thenReturn(new Index("test_index", "uuid"));
        when(mockContext.allowExpensiveQueries()).thenReturn(true);

        MapperService mockMapperService = mock(MapperService.class);
        when(mockContext.getMapperService()).thenReturn(mockMapperService);
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .build();
        // Mock IndexSettings
        IndexSettings mockIndexSettings = new IndexSettings(
            IndexMetadata.builder("test_index").settings(indexSettings).build(),
            Settings.EMPTY
        );
        when(mockMapperService.getIndexSettings()).thenReturn(mockIndexSettings);
        when(mockContext.getIndexSettings()).thenReturn(mockIndexSettings);
        docs = new ArrayList<>();
        for (String[] request : raw_requests) {
            Document document = new Document();
            document.add(new TextField("raw_request", request[0], Field.Store.YES));
            document.add(new KeywordField("status", request[1], Field.Store.YES));
            docs.add(document);
        }
    }

    public void testSimpleTermsAggregationWithDerivedField() throws IOException {
        MappedFieldType keywordFieldType = new KeywordFieldMapper.KeywordFieldType("status");

        SearchLookup searchLookup = mock(SearchLookup.class);
        SourceLookup sourceLookup = new SourceLookup();
        LeafSearchLookup leafLookup = mock(LeafSearchLookup.class);
        when(leafLookup.source()).thenReturn(sourceLookup);

        // Mock DerivedFieldScript.Factory
        DerivedFieldScript.Factory factory = (params, lookup) -> (DerivedFieldScript.LeafFactory) ctx -> {
            when(searchLookup.getLeafSearchLookup(any())).thenReturn(leafLookup);
            return new DerivedFieldScript(params, lookup, ctx) {
                @Override
                public void execute() {
                    addEmittedValue(raw_requests[sourceLookup.docId()][1]);
                }

                @Override
                public void setDocument(int docid) {
                    sourceLookup.setSegmentAndDocument(ctx, docid);
                }
            };
        };

        DerivedField derivedField = new DerivedField("derived_field", "keyword", new Script(""));
        DerivedFieldResolver resolver = DerivedFieldResolverFactory.createResolver(
            mockContext,
            Collections.emptyMap(),
            Collections.singletonList(derivedField),
            true
        );

        // spy on the resolved type so we can mock the valuefetcher
        DerivedFieldType derivedFieldType = spy((DerivedFieldType) resolver.resolve("derived_field"));
        DerivedFieldScript.LeafFactory leafFactory = factory.newFactory((new Script("")).getParams(), searchLookup);
        DerivedFieldValueFetcher valueFetcher = new DerivedFieldValueFetcher(leafFactory, null);
        doReturn(valueFetcher).when(derivedFieldType).valueFetcher(any(), any(), any());

        TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("derived_terms").field("status").size(10);

        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            for (Document d : docs) {
                iw.addDocument(d);
            }
        }, (InternalTerms result) -> {
            assertEquals(2, result.getBuckets().size());
            List<Terms.Bucket> buckets = result.getBuckets();
            assertEquals("200", buckets.get(0).getKey());
            assertEquals(3, buckets.get(0).getDocCount());
            assertEquals("400", buckets.get(1).getKey());
            assertEquals(3, buckets.get(1).getDocCount());
        }, keywordFieldType, derivedFieldType);
    }
}
