/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.fetch.subphase.highlight;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.document.DocumentField;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.ContentPath;
import org.opensearch.index.mapper.DefaultDerivedFieldResolver;
import org.opensearch.index.mapper.DerivedField;
import org.opensearch.index.mapper.DerivedFieldResolverFactory;
import org.opensearch.index.mapper.DerivedFieldSupportedTypes;
import org.opensearch.index.mapper.DerivedFieldType;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.SourceToParse;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.Rewriteable;
import org.opensearch.script.MockScriptEngine;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptEngine;
import org.opensearch.script.ScriptModule;
import org.opensearch.script.ScriptService;
import org.opensearch.script.ScriptType;
import org.opensearch.search.SearchHit;
import org.opensearch.search.fetch.FetchContext;
import org.opensearch.search.fetch.FetchSubPhase;
import org.opensearch.search.fetch.FetchSubPhaseProcessor;
import org.opensearch.search.fetch.subphase.FieldAndFormat;
import org.opensearch.search.fetch.subphase.FieldFetcher;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DerivedFieldFetchAndHighlightTests extends OpenSearchSingleNodeTestCase {
    private static String DERIVED_FIELD_SCRIPT_1 = "derived_field_script_1";
    private static String DERIVED_FIELD_SCRIPT_2 = "derived_field_script_2";
    private static String DERIVED_FIELD_SCRIPT_3 = "derived_field_script_3";
    private static String DERIVED_FIELD_SCRIPT_4 = "derived_field_script_4";

    private static String DERIVED_FIELD_1 = "derived_1";
    private static String DERIVED_FIELD_2 = "derived_2";
    private static String DERIVED_FIELD_3 = "derived_3";
    private static String DERIVED_FIELD_4 = "derived_4";
    private static String NESTED_FIELD = "field";

    public void testDerivedFieldFromIndexMapping() throws IOException {
        // Create index and mapper service
        // Define mapping for derived fields, create 2 derived fields derived_1 and derived_2
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("derived")
            .startObject(DERIVED_FIELD_1)
            .field("type", "keyword")
            .startObject("script")
            .field("source", DERIVED_FIELD_SCRIPT_1)
            .field("lang", "mockscript")
            .endObject()
            .endObject()
            .startObject(DERIVED_FIELD_2)
            .field("type", "keyword")
            .startObject("script")
            .field("source", DERIVED_FIELD_SCRIPT_2)
            .field("lang", "mockscript")
            .endObject()
            .endObject()
            .startObject(DERIVED_FIELD_3)
            .field("type", "date")
            .field("format", "yyyy-MM-dd")
            .startObject("script")
            .field("source", DERIVED_FIELD_SCRIPT_3)
            .field("lang", "mockscript")
            .endObject()
            .endObject()
            .startObject(DERIVED_FIELD_4)
            .field("type", "object")
            .startObject("script")
            .field("source", DERIVED_FIELD_SCRIPT_4)
            .field("lang", "mockscript")
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        // Create source document with 2 fields field1 and field2.
        // derived_1 will act on field1 and derived_2 will act on derived_2. DERIVED_FIELD_SCRIPT_1 substitutes whitespaces with _
        XContentBuilder source = XContentFactory.jsonBuilder()
            .startObject()
            .field("field1", "some_text_1")
            .field("field2", "some_text_2")
            .field("field3", 1710923445000L)
            .field("field4", "{ \"field\": \"foo bar baz\"}")
            .endObject();

        int docId = 0;
        IndexService indexService = createIndex("test_index", Settings.EMPTY, MapperService.SINGLE_MAPPING_NAME, mapping);
        MapperService mapperService = indexService.mapperService();

        try (
            Directory dir = newDirectory();
            RandomIndexWriter iw = new RandomIndexWriter(random(), dir, new IndexWriterConfig(mapperService.indexAnalyzer()));
        ) {
            iw.addDocument(
                mapperService.documentMapper()
                    .parse(new SourceToParse("test_index", "0", BytesReference.bytes(source), MediaTypeRegistry.JSON))
                    .rootDoc()
            );
            try (IndexReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                LeafReaderContext context = searcher.getIndexReader().leaves().get(0);
                QueryShardContext mockShardContext = createQueryShardContext(mapperService, searcher);
                mockShardContext.lookup().source().setSegmentAndDocument(context, docId);
                // mockShardContext.setDerivedFieldTypes(Map.of("derived_2", createDerivedFieldType("derived_1", "keyword"), "derived_1",

                // Assert the fetch phase works for both of the derived fields
                Map<String, DocumentField> fields = fetchFields(mockShardContext, context, "*");
                Map<String, DocumentField> nestedFields = fetchFields(mockShardContext, context, DERIVED_FIELD_4 + "." + NESTED_FIELD);

                // Validate FetchPhase
                {
                    assertEquals(fields.size(), 4);
                    assertEquals(1, fields.get(DERIVED_FIELD_1).getValues().size());
                    assertEquals(1, fields.get(DERIVED_FIELD_2).getValues().size());
                    assertEquals(1, fields.get(DERIVED_FIELD_3).getValues().size());
                    assertEquals(1, fields.get(DERIVED_FIELD_4).getValues().size());
                    assertEquals("some_text_1", fields.get(DERIVED_FIELD_1).getValue());
                    assertEquals("some_text_2", fields.get(DERIVED_FIELD_2).getValue());
                    assertEquals("2024-03-20", fields.get(DERIVED_FIELD_3).getValue());
                    assertEquals("{ \"field\": \"foo bar baz\"}", fields.get(DERIVED_FIELD_4).getValue());
                    assertEquals(1, nestedFields.get(DERIVED_FIELD_4 + "." + NESTED_FIELD).getValues().size());
                    assertEquals("foo bar baz", nestedFields.get(DERIVED_FIELD_4 + "." + NESTED_FIELD).getValue());
                }

                // Create a HighlightBuilder of type unified, set its fields as derived_1 and derived_2
                HighlightBuilder highlightBuilder = new HighlightBuilder();
                highlightBuilder.highlighterType("unified");
                highlightBuilder.field(DERIVED_FIELD_1);
                highlightBuilder.field(DERIVED_FIELD_2);
                highlightBuilder.field(DERIVED_FIELD_4 + "." + NESTED_FIELD);
                highlightBuilder = Rewriteable.rewrite(highlightBuilder, mockShardContext);
                SearchHighlightContext searchHighlightContext = highlightBuilder.build(mockShardContext);

                // Create a HighlightPhase with highlighter defined above
                HighlightPhase highlightPhase = new HighlightPhase(Collections.singletonMap("unified", new UnifiedHighlighter()));

                // create a fetch context to be used by HighlightPhase processor
                FetchContext fetchContext = mock(FetchContext.class);
                when(fetchContext.mapperService()).thenReturn(mapperService);
                when(fetchContext.getQueryShardContext()).thenReturn(mockShardContext);
                when(fetchContext.getIndexSettings()).thenReturn(indexService.getIndexSettings());
                when(fetchContext.searcher()).thenReturn(
                    new ContextIndexSearcher(
                        reader,
                        IndexSearcher.getDefaultSimilarity(),
                        IndexSearcher.getDefaultQueryCache(),
                        IndexSearcher.getDefaultQueryCachingPolicy(),
                        true,
                        null,
                        null
                    )
                );
                {
                    // The query used by FetchSubPhaseProcessor to highlight is a term query on DERIVED_FIELD_1
                    FetchSubPhaseProcessor subPhaseProcessor = highlightPhase.getProcessor(
                        fetchContext,
                        searchHighlightContext,
                        new TermQuery(new Term(DERIVED_FIELD_1, "some_text_1"))
                    );

                    // Create a search hit using the derived fields fetched above in fetch phase
                    SearchHit searchHit = new SearchHit(docId, "0", null, fields, null);

                    // Create a HitContext of search hit
                    FetchSubPhase.HitContext hitContext = new FetchSubPhase.HitContext(
                        searchHit,
                        context,
                        docId,
                        mockShardContext.lookup().source()
                    );
                    hitContext.sourceLookup().loadSourceIfNeeded();
                    // process the HitContext using the highlightPhase subPhaseProcessor
                    subPhaseProcessor.process(hitContext);

                    // Validate that 1 highlight field is present
                    assertEquals(hitContext.hit().getHighlightFields().size(), 1);
                }
                {
                    // The query used by FetchSubPhaseProcessor to highlight is a term query on DERIVED_FIELD_1
                    FetchSubPhaseProcessor subPhaseProcessor = highlightPhase.getProcessor(
                        fetchContext,
                        searchHighlightContext,
                        new TermQuery(new Term(DERIVED_FIELD_4 + "." + NESTED_FIELD, "foo"))
                    );

                    // Create a search hit using the derived fields fetched above in fetch phase
                    SearchHit searchHit = new SearchHit(docId, "0", null, nestedFields, null);

                    // Create a HitContext of search hit
                    FetchSubPhase.HitContext hitContext = new FetchSubPhase.HitContext(
                        searchHit,
                        context,
                        docId,
                        mockShardContext.lookup().source()
                    );
                    hitContext.sourceLookup().loadSourceIfNeeded();
                    // process the HitContext using the highlightPhase subPhaseProcessor
                    subPhaseProcessor.process(hitContext);

                    // Validate that 1 highlight field is present
                    assertEquals(hitContext.hit().getHighlightFields().size(), 1);
                }
            }
        }
    }

    public void testDerivedFieldFromSearchMapping() throws IOException {
        // Create source document with 2 fields field1 and field2.
        // derived_1 will act on field1 and derived_2 will act on derived_2. DERIVED_FIELD_SCRIPT_1 substitutes whitespaces with _
        XContentBuilder source = XContentFactory.jsonBuilder()
            .startObject()
            .field("field1", "some_text_1")
            .field("field2", "some_text_2")
            .field("field3", 1710923445000L)
            .field("field4", "{ \"field\": \"foo bar baz\"}")
            .endObject();

        int docId = 0;

        // Create index and mapper service
        // We are not defining derived fields in index mapping here
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().endObject();
        IndexService indexService = createIndex("test_index", Settings.EMPTY, MapperService.SINGLE_MAPPING_NAME, mapping);
        MapperService mapperService = indexService.mapperService();

        try (
            Directory dir = newDirectory();
            RandomIndexWriter iw = new RandomIndexWriter(random(), dir, new IndexWriterConfig(mapperService.indexAnalyzer()));
        ) {
            iw.addDocument(
                mapperService.documentMapper()
                    .parse(new SourceToParse("test_index", "0", BytesReference.bytes(source), MediaTypeRegistry.JSON))
                    .rootDoc()
            );
            try (IndexReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                LeafReaderContext context = searcher.getIndexReader().leaves().get(0);
                QueryShardContext mockShardContext = createQueryShardContext(mapperService, searcher);
                mockShardContext.lookup().source().setSegmentAndDocument(context, docId);

                DerivedField derivedField3 = new DerivedField(
                    DERIVED_FIELD_3,
                    "date",
                    new Script(ScriptType.INLINE, "mockscript", DERIVED_FIELD_SCRIPT_3, emptyMap())
                );
                derivedField3.setFormat("dd-MM-yyyy");
                // This mock behavior is similar to adding derived fields in search request
                mockShardContext.setDerivedFieldResolver(
                    (DefaultDerivedFieldResolver) DerivedFieldResolverFactory.createResolver(
                        mockShardContext,
                        null,
                        List.of(
                            new DerivedField(
                                DERIVED_FIELD_1,
                                "keyword",
                                new Script(ScriptType.INLINE, "mockscript", DERIVED_FIELD_SCRIPT_1, emptyMap())
                            ),
                            new DerivedField(
                                DERIVED_FIELD_2,
                                "keyword",
                                new Script(ScriptType.INLINE, "mockscript", DERIVED_FIELD_SCRIPT_2, emptyMap())
                            ),
                            derivedField3,
                            new DerivedField(
                                DERIVED_FIELD_4,
                                "object",
                                new Script(ScriptType.INLINE, "mockscript", DERIVED_FIELD_SCRIPT_4, emptyMap())
                            )
                        ),
                        true
                    )
                );

                // Assert the fetch phase works for both of the derived fields
                Map<String, DocumentField> fields = fetchFields(mockShardContext, context, "derived_*");
                Map<String, DocumentField> nestedFields = fetchFields(mockShardContext, context, DERIVED_FIELD_4 + "." + NESTED_FIELD);

                // Validate FetchPhase
                {
                    assertEquals(fields.size(), 4);
                    assertEquals(1, fields.get(DERIVED_FIELD_1).getValues().size());
                    assertEquals(1, fields.get(DERIVED_FIELD_2).getValues().size());
                    assertEquals(1, fields.get(DERIVED_FIELD_3).getValues().size());
                    assertEquals(1, fields.get(DERIVED_FIELD_4).getValues().size());
                    assertEquals("some_text_1", fields.get(DERIVED_FIELD_1).getValue());
                    assertEquals("some_text_2", fields.get(DERIVED_FIELD_2).getValue());
                    assertEquals("20-03-2024", fields.get(DERIVED_FIELD_3).getValue());
                    assertEquals("{ \"field\": \"foo bar baz\"}", fields.get(DERIVED_FIELD_4).getValue());
                    assertEquals(1, nestedFields.get(DERIVED_FIELD_4 + "." + NESTED_FIELD).getValues().size());
                    assertEquals("foo bar baz", nestedFields.get(DERIVED_FIELD_4 + "." + NESTED_FIELD).getValue());
                }

                // Create a HighlightBuilder of type unified, set its fields as derived_1 and derived_2
                HighlightBuilder highlightBuilder = new HighlightBuilder();
                highlightBuilder.highlighterType("unified");
                highlightBuilder.field(DERIVED_FIELD_1);
                highlightBuilder.field(DERIVED_FIELD_2);
                highlightBuilder.field(DERIVED_FIELD_4 + "." + NESTED_FIELD);
                highlightBuilder = Rewriteable.rewrite(highlightBuilder, mockShardContext);
                SearchHighlightContext searchHighlightContext = highlightBuilder.build(mockShardContext);

                // Create a HighlightPhase with highlighter defined above
                HighlightPhase highlightPhase = new HighlightPhase(Collections.singletonMap("unified", new UnifiedHighlighter()));

                // create a fetch context to be used by HighlightPhase processor
                FetchContext fetchContext = mock(FetchContext.class);
                when(fetchContext.mapperService()).thenReturn(mapperService);
                when(fetchContext.getQueryShardContext()).thenReturn(mockShardContext);
                when(fetchContext.getIndexSettings()).thenReturn(indexService.getIndexSettings());
                when(fetchContext.searcher()).thenReturn(
                    new ContextIndexSearcher(
                        reader,
                        IndexSearcher.getDefaultSimilarity(),
                        IndexSearcher.getDefaultQueryCache(),
                        IndexSearcher.getDefaultQueryCachingPolicy(),
                        true,
                        null,
                        null
                    )
                );
                {
                    // The query used by FetchSubPhaseProcessor to highlight is a term query on DERIVED_FIELD_1
                    FetchSubPhaseProcessor subPhaseProcessor = highlightPhase.getProcessor(
                        fetchContext,
                        searchHighlightContext,
                        new TermQuery(new Term(DERIVED_FIELD_1, "some_text_1"))
                    );

                    // Create a search hit using the derived fields fetched above in fetch phase
                    SearchHit searchHit = new SearchHit(docId, "0", null, fields, null);

                    // Create a HitContext of search hit
                    FetchSubPhase.HitContext hitContext = new FetchSubPhase.HitContext(
                        searchHit,
                        context,
                        docId,
                        mockShardContext.lookup().source()
                    );
                    hitContext.sourceLookup().loadSourceIfNeeded();
                    // process the HitContext using the highlightPhase subPhaseProcessor
                    subPhaseProcessor.process(hitContext);

                    // Validate that 1 highlight field is present
                    assertEquals(hitContext.hit().getHighlightFields().size(), 1);
                }
                {
                    // test highlighting nested field DERIVED_FIELD_4 + "." + NESTED_FIELD
                    FetchSubPhaseProcessor subPhaseProcessor = highlightPhase.getProcessor(
                        fetchContext,
                        searchHighlightContext,
                        new TermQuery(new Term(DERIVED_FIELD_4 + "." + NESTED_FIELD, "foo"))
                    );

                    // Create a search hit using the derived fields fetched above in fetch phase
                    SearchHit searchHit = new SearchHit(docId, "0", null, nestedFields, null);

                    // Create a HitContext of search hit
                    FetchSubPhase.HitContext hitContext = new FetchSubPhase.HitContext(
                        searchHit,
                        context,
                        docId,
                        mockShardContext.lookup().source()
                    );
                    hitContext.sourceLookup().loadSourceIfNeeded();
                    // process the HitContext using the highlightPhase subPhaseProcessor
                    subPhaseProcessor.process(hitContext);

                    // Validate that 1 highlight field is present
                    assertEquals(hitContext.hit().getHighlightFields().size(), 1);
                }
            }
        }
    }

    public static Map<String, DocumentField> fetchFields(
        QueryShardContext queryShardContext,
        LeafReaderContext context,
        String fieldPattern
    ) throws IOException {
        List<FieldAndFormat> fields = List.of(new FieldAndFormat(fieldPattern, null));
        FieldFetcher fieldFetcher = FieldFetcher.create(queryShardContext, queryShardContext.lookup(), fields);
        fieldFetcher.setNextReader(context);
        return fieldFetcher.fetch(queryShardContext.lookup().source(), Set.of());
    }

    private static QueryShardContext createQueryShardContext(MapperService mapperService, IndexSearcher indexSearcher) {
        Settings settings = Settings.builder()
            .put("index.version.created", Version.CURRENT)
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put(IndexMetadata.SETTING_INDEX_UUID, "uuid")
            .build();
        IndexMetadata indexMetadata = new IndexMetadata.Builder("index").settings(settings).build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, settings);

        ScriptService scriptService = getScriptService();
        return new QueryShardContext(
            0,
            indexSettings,
            null,
            null,
            null,
            mapperService,
            null,
            scriptService,
            null,
            null,
            null,
            indexSearcher,
            null,
            null,
            null,
            () -> true,
            null
        );
    }

    private static ScriptService getScriptService() {
        final MockScriptEngine engine = new MockScriptEngine(
            MockScriptEngine.NAME,
            Map.of(
                DERIVED_FIELD_SCRIPT_1,
                (script) -> ((String) ((Map<String, Object>) script.get("_source")).get("field1")).replace(" ", "_"),
                DERIVED_FIELD_SCRIPT_2,
                (script) -> ((String) ((Map<String, Object>) script.get("_source")).get("field2")).replace(" ", "_"),
                DERIVED_FIELD_SCRIPT_3,
                (script) -> ((Map<String, Object>) script.get("_source")).get("field3"),
                DERIVED_FIELD_SCRIPT_4,
                (script) -> ((Map<String, Object>) script.get("_source")).get("field4")
            ),
            Collections.emptyMap()
        );
        final Map<String, ScriptEngine> engines = singletonMap(engine.getType(), engine);
        ScriptService scriptService = new ScriptService(Settings.EMPTY, engines, ScriptModule.CORE_CONTEXTS);
        return scriptService;
    }

    private DerivedFieldType createDerivedFieldType(String name, String type, String script) {
        Mapper.BuilderContext context = mock(Mapper.BuilderContext.class);
        when(context.path()).thenReturn(new ContentPath());
        return new DerivedFieldType(
            new DerivedField(name, type, new Script(ScriptType.INLINE, "mockscript", script, emptyMap())),
            DerivedFieldSupportedTypes.getFieldMapperFromType(type, name, context, null),
            DerivedFieldSupportedTypes.getIndexableFieldGeneratorType(type, name),
            null
        );
    }
}
