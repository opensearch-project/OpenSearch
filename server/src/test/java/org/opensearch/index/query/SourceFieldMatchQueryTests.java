/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.query;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
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
            b.startObject("desert");
            {
                b.field("type", "match_only_text");
            }
            b.endObject();
        }));

        QueryShardContext queryShardContext = createQueryShardContext(mapperService);
        when(queryShardContext.sourcePath("desert")).thenReturn(Set.of("desert"));

        String[] deserts = new String[] { "apple pie pie", "banana split pie", "chocolate cake" };
        List<ParsedDocument> docs = new ArrayList<>();
        for (String desert : deserts) {
            docs.add(mapperService.documentMapper().parse(source(b -> b.field("desert", desert))));
        }
        SourceFieldMatchQuery matchBoth = new SourceFieldMatchQuery(
            QueryBuilders.matchQuery("desert", "apple").doToQuery(queryShardContext),  // Delegate query
            QueryBuilders.matchQuery("desert", "pie").doToQuery(queryShardContext),    // Filter query
            queryShardContext.getFieldType("desert"),
            queryShardContext
        );

        SourceFieldMatchQuery matchDelegate = new SourceFieldMatchQuery(
            QueryBuilders.matchQuery("desert", "apple").doToQuery(queryShardContext),  // Delegate query
            QueryBuilders.matchQuery("desert", "juice").doToQuery(queryShardContext),    // Filter query
            queryShardContext.getFieldType("desert"),
            queryShardContext
        );

        SourceFieldMatchQuery matchFilter = new SourceFieldMatchQuery(
            QueryBuilders.matchQuery("desert", "tart").doToQuery(queryShardContext),  // Delegate query
            QueryBuilders.matchQuery("desert", "pie").doToQuery(queryShardContext),    // Filter query
            queryShardContext.getFieldType("desert"),
            queryShardContext
        );

        SourceFieldMatchQuery matchNone = new SourceFieldMatchQuery(
            QueryBuilders.matchQuery("desert", "gulab").doToQuery(queryShardContext),  // Delegate query
            QueryBuilders.matchQuery("desert", "jamun").doToQuery(queryShardContext),    // Filter query
            queryShardContext.getFieldType("desert"),
            queryShardContext
        );

        SourceFieldMatchQuery matchMultipleDocs = new SourceFieldMatchQuery(
            QueryBuilders.matchAllQuery().toQuery(queryShardContext),  // Delegate query
            QueryBuilders.matchQuery("desert", "pie").doToQuery(queryShardContext),    // Filter query
            queryShardContext.getFieldType("desert"),
            queryShardContext
        );

        withLuceneIndex(mapperService, iw -> {
            for (ParsedDocument d : docs) {
                iw.addDocument(d.rootDoc());
            }
        }, reader -> {
            IndexSearcher searcher = newSearcher(reader);
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
        });
    }
}
