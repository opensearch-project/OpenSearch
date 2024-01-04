/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.query;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.opensearch.core.index.Index;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.MapperServiceTestCase;
import org.opensearch.index.mapper.ParsedDocument;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.mockito.Mockito.when;

public class SourceFieldMatchQueryTests extends MapperServiceTestCase {

    public void testAllPossibleScenarios() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("dessert");
            {
                b.field("type", "match_only_text");
            }
            b.endObject();
        }));

        QueryShardContext queryShardContext = createQueryShardContext(mapperService);
        when(queryShardContext.sourcePath("dessert")).thenReturn(Set.of("dessert"));
        when(queryShardContext.index()).thenReturn(new Index("test_index", "uuid"));

        String[] desserts = new String[] { "apple pie pie", "banana split pie", "chocolate cake" };
        List<ParsedDocument> docs = new ArrayList<>();
        for (String dessert : desserts) {
            docs.add(mapperService.documentMapper().parse(source(b -> b.field("dessert", dessert))));
        }
        SourceFieldMatchQuery matchBoth = new SourceFieldMatchQuery(
            QueryBuilders.matchQuery("dessert", "apple").doToQuery(queryShardContext),  // Delegate query
            QueryBuilders.matchQuery("dessert", "pie").doToQuery(queryShardContext),    // Filter query
            queryShardContext.getFieldType("dessert"),
            queryShardContext
        );

        SourceFieldMatchQuery matchDelegate = new SourceFieldMatchQuery(
            QueryBuilders.matchQuery("dessert", "apple").doToQuery(queryShardContext),  // Delegate query
            QueryBuilders.matchQuery("dessert", "juice").doToQuery(queryShardContext),    // Filter query
            queryShardContext.getFieldType("dessert"),
            queryShardContext
        );

        SourceFieldMatchQuery matchFilter = new SourceFieldMatchQuery(
            QueryBuilders.matchQuery("dessert", "tart").doToQuery(queryShardContext),  // Delegate query
            QueryBuilders.matchQuery("dessert", "pie").doToQuery(queryShardContext),    // Filter query
            queryShardContext.getFieldType("dessert"),
            queryShardContext
        );

        SourceFieldMatchQuery matchNone = new SourceFieldMatchQuery(
            QueryBuilders.matchQuery("dessert", "gulab").doToQuery(queryShardContext),  // Delegate query
            QueryBuilders.matchQuery("dessert", "jamun").doToQuery(queryShardContext),    // Filter query
            queryShardContext.getFieldType("dessert"),
            queryShardContext
        );

        SourceFieldMatchQuery matchMultipleDocs = new SourceFieldMatchQuery(
            QueryBuilders.matchAllQuery().toQuery(queryShardContext),  // Delegate query
            QueryBuilders.matchQuery("dessert", "pie").doToQuery(queryShardContext),    // Filter query
            queryShardContext.getFieldType("dessert"),
            queryShardContext
        );
        try (Directory dir = newDirectory()) {
            IndexWriter iw = new IndexWriter(dir, new IndexWriterConfig(mapperService.indexAnalyzer()));
            for (ParsedDocument d : docs) {
                iw.addDocument(d.rootDoc());
            }
            try (IndexReader reader = DirectoryReader.open(iw)) {
                iw.close();
                IndexSearcher searcher = new IndexSearcher(reader);
                TopDocs topDocs = searcher.search(matchBoth, 10);
                assertEquals(topDocs.totalHits.value, 1);
                assertEquals(topDocs.scoreDocs[0].doc, 0);

                topDocs = searcher.search(matchDelegate, 10);
                assertEquals(topDocs.totalHits.value, 0);

                topDocs = searcher.search(matchFilter, 10);
                assertEquals(topDocs.totalHits.value, 0);

                topDocs = searcher.search(matchNone, 10);
                assertEquals(topDocs.totalHits.value, 0);

                topDocs = searcher.search(matchMultipleDocs, 10);
                assertEquals(topDocs.totalHits.value, 2);
                // assert constant score
                for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                    assertEquals(scoreDoc.score, 1.0, 0.00000000001);
                }
            }
        }
    }

    public void testSourceDisabled() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> b.startObject("_source").field("enabled", false).endObject()));
        QueryShardContext queryShardContext = createQueryShardContext(mapperService);
        when(queryShardContext.sourcePath("dessert")).thenReturn(Set.of("dessert"));
        when(queryShardContext.index()).thenReturn(new Index("test_index", "uuid"));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new SourceFieldMatchQuery(
                QueryBuilders.matchQuery("dessert", "apple").doToQuery(queryShardContext),  // Delegate query
                QueryBuilders.matchQuery("dessert", "pie").doToQuery(queryShardContext),    // Filter query
                queryShardContext.getFieldType("dessert"),
                queryShardContext
            )
        );
        assertEquals(
            "SourceFieldMatchQuery error: unable to fetch fields from _source field: "
                + "_source is disabled in the mappings for index [test_index]",
            e.getMessage()
        );
    }

    public void testMissingField() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("dessert");
            {
                b.field("type", "match_only_text");
            }
            b.endObject();
        }));
        QueryShardContext queryShardContext = createQueryShardContext(mapperService);
        when(queryShardContext.sourcePath("dessert")).thenReturn(Set.of("dessert"));
        when(queryShardContext.index()).thenReturn(new Index("test_index", "uuid"));

        String[] desserts = new String[] { "apple pie pie", "banana split pie", "chocolate cake" };
        List<ParsedDocument> docs = new ArrayList<>();
        for (String dessert : desserts) {
            docs.add(mapperService.documentMapper().parse(source(b -> b.field("dessert", dessert))));
        }
        SourceFieldMatchQuery matchDelegate = new SourceFieldMatchQuery(
            QueryBuilders.matchQuery("dessert", "apple").doToQuery(queryShardContext),  // Delegate query
            QueryBuilders.matchQuery("username", "pie").doToQuery(queryShardContext),    // Filter query missing field
            queryShardContext.getFieldType("dessert"),
            queryShardContext
        );
        try (Directory dir = newDirectory()) {
            IndexWriter iw = new IndexWriter(dir, new IndexWriterConfig(mapperService.indexAnalyzer()));
            for (ParsedDocument d : docs) {
                iw.addDocument(d.rootDoc());
            }
            try (IndexReader reader = DirectoryReader.open(iw)) {
                iw.close();
                IndexSearcher searcher = new IndexSearcher(reader);
                TopDocs topDocs = searcher.search(matchDelegate, 10);
                assertEquals(topDocs.totalHits.value, 0);
            }
        }
    }
}
