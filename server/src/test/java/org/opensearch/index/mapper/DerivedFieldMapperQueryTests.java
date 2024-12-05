/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.opensearch.OpenSearchParseException;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.core.index.Index;
import org.opensearch.geometry.Rectangle;
import org.opensearch.index.query.MatchPhrasePrefixQueryBuilder;
import org.opensearch.index.query.MultiMatchQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.search.QueryStringQueryParser;
import org.opensearch.script.DerivedFieldScript;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.mockito.Mockito;

import static org.opensearch.index.query.QueryBuilders.geoShapeQuery;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

public class DerivedFieldMapperQueryTests extends MapperServiceTestCase {

    // First element is the document ingested, other elements corresponds to value returned against a given derived field script
    // Raw Message, Request Succeeded (boolean), Timestamp (long), Client IP, Method, Request Size (double), Duration (long)
    private static final Object[][] raw_requests = new Object[][] {
        {
            "40.135.0.0 GET /images/hm_bg.jpg?size=1.5KB loc 10.0 20.0 HTTP/1.0 200 2024-03-20T08:30:45 1500",
            true,
            1710923445000L,
            "40.135.0.0",
            "GET",
            1.5,
            1500L,
            new Tuple<>(10.0, 20.0),
            "{ "
                + "\"float_field\": 1.5,"
                + "\"boolean_field\": true,"
                + "\"long_field\": 1500,"
                + "\"date_field\": \"2024-03-20T08:30:45\","
                + "\"keyword_field\": \"GET\","
                + "\"array_field\": [1, 2, 3],"
                + "\"text_field\": \"document number 1\","
                + "\"nested_field\": { \"sub_field_1\": \"GET\"}"
                + "}" },
        {
            "232.0.0.0 GET /images/hm_bg.jpg?size=2.3KB HTTP/1.0 400 2024-03-20T09:15:20 2300",
            false,
            1710926120000L,
            "232.0.0.0",
            "GET",
            2.3,
            2300L,
            new Tuple<>(20.0, 30.0),
            "{ "
                + "\"float_field\": 2.3,"
                + "\"boolean_field\": false,"
                + "\"long_field\": 2300,"
                + "\"date_field\": \"2024-03-20T09:15:20\","
                + "\"keyword_field\": \"GET\","
                + "\"array_field\": [4, 5, 6],"
                + "\"text_field\": \"document number 2\","
                + "\"nested_field\": { \"sub_field_1\": \"GET\"}"
                + "}" },
        {
            "26.1.0.0 DELETE /images/hm_bg.jpg?size=3.7KB HTTP/1.0 200 2024-03-20T10:05:55 3700",
            true,
            1710929155000L,
            "26.1.0.0",
            "DELETE",
            3.7,
            3700L,
            new Tuple<>(30.0, 40.0),
            "{ "
                + "\"float_field\": 3.7,"
                + "\"boolean_field\": true,"
                + "\"long_field\": 3700,"
                + "\"date_field\": \"2024-03-20T10:05:55\","
                + "\"array_field\": [7, 8, 9],"
                + "\"keyword_field\": \"DELETE\","
                + "\"text_field\": \"document number 3\","
                + "\"nested_field\": { \"sub_field_1\": \"DELETE\"}"
                + "}" },
        {
            "247.37.0.0 GET /french/splash_inet.html?size=4.1KB HTTP/1.0 400 2024-03-20T11:20:10 4100",
            false,
            1710933610000L,
            "247.37.0.0",
            "GET",
            4.1,
            4100L,
            new Tuple<>(40.0, 50.0),
            "{ "
                + "\"float_field\": 4.1,"
                + "\"boolean_field\": false,"
                + "\"long_field\": 4100,"
                + "\"date_field\": \"2024-03-20T11:20:10\","
                + "\"keyword_field\": \"GET\","
                + "\"array_field\": [10, 11, 12],"
                + "\"text_field\": \"document number 4\","
                + "\"nested_field\": { \"sub_field_1\": \"GET\"}"
                + "}" },
        {
            "247.37.0.0 DELETE /french/splash_inet.html?size=5.8KB HTTP/1.0 400 2024-03-20T12:45:30 5800",
            false,
            1710938730000L,
            "247.37.0.0",
            "DELETE",
            5.8,
            5800L,
            new Tuple<>(50.0, 60.0),
            "{ "
                + "\"float_field\": 5.8,"
                + "\"boolean_field\": false,"
                + "\"long_field\": 5800,"
                + "\"date_field\": \"2024-03-20T12:45:30\","
                + "\"keyword_field\": \"DELETE\","
                + "\"array_field\": [13, 14, 15],"
                + "\"text_field\": \"document number 5\","
                + "\"nested_field\": { \"sub_field_1\": \"DELETE\"}"
                + "}" },
        {
            "10.20.30.40 GET /path/to/resource?size=6.3KB HTTP/1.0 200 2024-03-20T13:10:15 6300",
            true,
            1710940215000L,
            "10.20.30.40",
            "GET",
            6.3,
            6300L,
            new Tuple<>(60.0, 70.0),
            "{ "
                + "\"float_field\": 6.3,"
                + "\"boolean_field\": true,"
                + "\"long_field\": 6300,"
                + "\"date_field\": \"2024-03-20T13:10:15\","
                + "\"keyword_field\": \"GET\","
                + "\"array_field\": [16, 17, 18],"
                + "\"text_field\": \"document number 6\","
                + "\"nested_field\": { \"sub_field_1\": \"GET\"}"
                + "}" },
        {
            "50.60.70.80 GET /path/to/resource?size=7.2KB HTTP/1.0 404 2024-03-20T14:20:50 7200",
            false,
            1710944450000L,
            "50.60.70.80",
            "GET",
            7.2,
            7200L,
            new Tuple<>(70.0, 80.0),
            "{ "
                + "\"float_field\": 7.2,"
                + "\"boolean_field\": false,"
                + "\"long_field\": 7200,"
                + "\"date_field\": \"2024-03-20T14:20:50\","
                + "\"keyword_field\": \"GET\","
                + "\"array_field\": [19, 20, 21],"
                + "\"text_field\": \"document number 7\","
                + "\"nested_field\": { \"sub_field_1\": \"GET\"}"
                + "}" },
        {
            "127.0.0.1 PUT /path/to/resource?size=8.9KB HTTP/1.0 500 2024-03-20T15:30:25 8900",
            false,
            1710948625000L,
            "127.0.0.1",
            "PUT",
            8.9,
            8900L,
            new Tuple<>(80.0, 90.0),
            "{ "
                + "\"float_field\": 8.9,"
                + "\"boolean_field\": false,"
                + "\"long_field\": 8900,"
                + "\"date_field\": \"2024-03-20T15:30:25\","
                + "\"keyword_field\": \"PUT\","
                + "\"array_field\": [22, 23, 24],"
                + "\"text_field\": \"document number 8\","
                + "\"nested_field\": { \"sub_field_1\": \"PUT\"}"
                + "}" },
        {
            "127.0.0.1 GET /path/to/resource?size=9.4KB HTTP/1.0 200 2024-03-20T16:40:15 9400",
            true,
            1710952815000L,
            "127.0.0.1",
            "GET",
            9.4,
            9400L,
            new Tuple<>(85.0, 90.0),
            "{ "
                + "\"float_field\": 9.4,"
                + "\"boolean_field\": true,"
                + "\"long_field\": 9400,"
                + "\"date_field\": \"2024-03-20T16:40:15\","
                + "\"keyword_field\": \"GET\","
                + "\"array_field\": [25, 26, 27],"
                + "\"text_field\": \"document number 9\","
                + "\"nested_field\": { \"sub_field_1\": \"GET\"}"
                + "}" },
        {
            "192.168.1.1 GET /path/to/resource?size=10.7KB HTTP/1.0 400 2024-03-20T17:50:40 10700",
            false,
            1710957040000L,
            "192.168.1.1",
            "GET",
            10.7,
            10700L,
            new Tuple<>(90.0, 90.0),
            "{ "
                + "\"float_field\": 10.7,"
                + "\"boolean_field\": false,"
                + "\"long_field\": 10700,"
                + "\"date_field\": \"2024-03-20T17:50:40\","
                + "\"keyword_field\": \"GET\","
                + "\"array_field\": [28, 29, 30],"
                + "\"text_field\": \"document number 10\","
                + "\"invalid_field\": {},"
                + "\"nested_field\": { \"sub_field_1\": \"GET\"}"
                + "}" },
        // malformed json
        {
            "192.168.1.1 GET /path/to/resource?size=10.7KB HTTP/1.0 400 2024-03-20T17:50:40 10700",
            false,
            -1L,
            "192.169.1.1",
            "JKJ",
            -1,
            -1,
            new Tuple<>(-1.0, -1.0),
            "{ "
                + "\"float_field\": 10.7,"
                + "\"boolean_field\": false,"
                + "\"long_field\": 10700,"
                + "\"date_field\": \"2024-03-20T17:50:40\","
                + "\"keyword_field\": \"GET\","
                + "\"array_field\": [28, 29, 30],"
                + "\"text_field\": \"document number 10\","
                + "\"invalid_field\": {},"
                + "\"nested_field\": { \"sub_field_1\": \"GET\"}"
                + "" } };

    public void testAllPossibleQueriesOnDerivedFields() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.startObject("properties");
            {
                b.startObject("raw_message");
                {
                    b.field("type", "text");
                }
                b.endObject();
            }
            b.endObject();
            b.startObject("derived");
            {
                b.startObject("request_succeeded");
                {
                    b.field("type", "boolean");
                    b.field("script", "");
                }
                b.endObject();
                b.startObject("@timestamp");
                {
                    b.field("type", "date");
                    b.field("script", "");
                    b.field("format", "yyyy-MM-dd");
                }
                b.endObject();
                b.startObject("client_ip");
                {
                    b.field("type", "ip");
                    b.field("script", "");
                }
                b.endObject();
                b.startObject("method");
                {
                    b.field("type", "keyword");
                    b.field("script", "");
                }
                b.endObject();
                b.startObject("request_size");
                {
                    b.field("type", "double");
                    b.field("script", "");
                }
                b.endObject();
                b.startObject("duration");
                {
                    b.field("type", "long");
                    b.field("script", "");
                }
                b.endObject();
                b.startObject("geopoint");
                {
                    b.field("type", "geo_point");
                    b.field("script", "");
                }
                b.endObject();
                b.startObject("object_field");
                {
                    b.field("type", "object");
                    b.field("script", "");
                }
                b.endObject();
            }
            b.endObject();
        }));

        List<Document> docs = new ArrayList<>();
        for (Object[] request : raw_requests) {
            Document document = new Document();
            document.add(new TextField("raw_message", (String) request[0], Field.Store.YES));
            docs.add(document);
        }

        int[] scriptIndex = { 1 };

        // Mock DerivedFieldScript.Factory
        DerivedFieldScript.Factory factory = (params, lookup) -> (DerivedFieldScript.LeafFactory) ctx -> new DerivedFieldScript(
            params,
            lookup,
            ctx
        ) {
            int docId = 0;

            @Override
            public void setDocument(int docId) {
                super.setDocument(docId);
                this.docId = docId;
            }

            @Override
            public void execute() {
                addEmittedValue(raw_requests[docId][scriptIndex[0]]);
            }
        };

        QueryShardContext queryShardContext = createQueryShardContext(mapperService);
        when(queryShardContext.compile(Mockito.any(), Mockito.any())).thenReturn(factory);
        when(queryShardContext.sourcePath("raw_message")).thenReturn(Set.of("raw_message"));
        when(queryShardContext.index()).thenReturn(new Index("test_index", "uuid"));

        // Index and Search
        try (Directory dir = newDirectory()) {
            IndexWriter iw = new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER));
            for (Document d : docs) {
                iw.addDocument(d);
            }
            try (IndexReader reader = DirectoryReader.open(iw)) {
                iw.close();

                IndexSearcher searcher = new IndexSearcher(reader);
                Query query = QueryBuilders.termQuery("request_succeeded", "true").toQuery(queryShardContext);
                TopDocs topDocs = searcher.search(query, 10);
                assertEquals(4, topDocs.totalHits.value);

                // IP Field Term Query
                scriptIndex[0] = 3;
                query = QueryBuilders.termQuery("client_ip", "192.168.0.0/16").toQuery(queryShardContext);
                topDocs = searcher.search(query, 10);
                assertEquals(1, topDocs.totalHits.value);

                scriptIndex[0] = 4;
                query = QueryBuilders.termsQuery("method", "DELETE", "PUT").toQuery(queryShardContext);
                topDocs = searcher.search(query, 10);
                assertEquals(3, topDocs.totalHits.value);

                query = QueryBuilders.termsQuery("method", "delete").toQuery(queryShardContext);
                topDocs = searcher.search(query, 10);
                assertEquals(0, topDocs.totalHits.value);

                query = QueryBuilders.termQuery("method", "delete").caseInsensitive(true).toQuery(queryShardContext);
                topDocs = searcher.search(query, 10);
                assertEquals(2, topDocs.totalHits.value);

                // Range queries of types - date, long and double
                scriptIndex[0] = 2;
                query = QueryBuilders.rangeQuery("@timestamp").from("2024-03-20T14:20:50").toQuery(queryShardContext);
                topDocs = searcher.search(query, 10);
                assertEquals(4, topDocs.totalHits.value);

                scriptIndex[0] = 5;
                query = QueryBuilders.rangeQuery("request_size").from("4.1").toQuery(queryShardContext);
                topDocs = searcher.search(query, 10);
                assertEquals(7, topDocs.totalHits.value);

                scriptIndex[0] = 6;
                query = QueryBuilders.rangeQuery("duration").from("5800").toQuery(queryShardContext);
                topDocs = searcher.search(query, 10);
                assertEquals(6, topDocs.totalHits.value);

                scriptIndex[0] = 4;

                // Prefix Query
                query = QueryBuilders.prefixQuery("method", "DE").toQuery(queryShardContext);
                topDocs = searcher.search(query, 10);
                assertEquals(2, topDocs.totalHits.value);

                scriptIndex[0] = 4;
                query = QueryBuilders.wildcardQuery("method", "G*").toQuery(queryShardContext);
                topDocs = searcher.search(query, 10);
                assertEquals(7, topDocs.totalHits.value);

                // Regexp Query
                scriptIndex[0] = 4;
                query = QueryBuilders.regexpQuery("method", ".*LET.*").toQuery(queryShardContext);
                topDocs = searcher.search(query, 10);
                assertEquals(2, topDocs.totalHits.value);

                // GeoPoint Query
                scriptIndex[0] = 7;

                query = geoShapeQuery("geopoint", new Rectangle(0.0, 55.0, 55.0, 0.0)).toQuery(queryShardContext);
                topDocs = searcher.search(query, 10);
                assertEquals(4, topDocs.totalHits.value);
            }
        }
    }

    public void testObjectDerivedFields() throws IOException, ParseException {
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.startObject("properties");
            {
                b.startObject("raw_message");
                {
                    b.field("type", "text");
                }
                b.endObject();
            }
            b.endObject();
            b.startObject("derived");
            {
                b.startObject("object_field");
                {
                    b.field("type", "object");
                    b.field("script", "");
                    b.field("ignore_malformed", "true");
                }
                b.endObject();
                b.startObject("object_field_without_ignored_malformed");
                {
                    b.field("type", "object");
                    b.field("script", "");
                }
                b.endObject();
            }
            b.endObject();
        }));

        List<Document> docs = new ArrayList<>();
        for (Object[] request : raw_requests) {
            Document document = new Document();
            document.add(new TextField("raw_message", (String) request[0], Field.Store.YES));
            docs.add(document);
        }

        int[] scriptIndex = { 8 };

        // Mock DerivedFieldScript.Factory
        DerivedFieldScript.Factory factory = (params, lookup) -> (DerivedFieldScript.LeafFactory) ctx -> new DerivedFieldScript(
            params,
            lookup,
            ctx
        ) {
            int docId = 0;

            @Override
            public void setDocument(int docId) {
                super.setDocument(docId);
                this.docId = docId;
            }

            @Override
            public void execute() {
                addEmittedValue(raw_requests[docId][scriptIndex[0]]);
            }
        };

        // Index and Search
        try (Directory dir = newDirectory()) {
            IndexWriter iw = new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER));
            for (Document d : docs) {
                iw.addDocument(d);
            }
            try (IndexReader reader = DirectoryReader.open(iw)) {
                iw.close();
                QueryShardContext queryShardContext = createQueryShardContext(mapperService);
                when(queryShardContext.compile(Mockito.any(), Mockito.any())).thenReturn(factory);
                when(queryShardContext.sourcePath("raw_message")).thenReturn(Set.of("raw_message"));
                when(queryShardContext.index()).thenReturn(new Index("test_index", "uuid"));
                when(queryShardContext.getIndexReader()).thenReturn(reader);
                when(queryShardContext.fieldMapper(anyString())).thenAnswer(inv -> {
                    MappedFieldType res = mapperService.fieldType(inv.getArguments()[0].toString());
                    if (res == null) {
                        DerivedFieldResolver derivedFieldResolver = DerivedFieldResolverFactory.createResolver(
                            queryShardContext,
                            null,
                            null,
                            true
                        );
                        res = derivedFieldResolver.resolve(inv.getArguments()[0].toString());
                    }
                    return res;
                });
                IndexSearcher searcher = new IndexSearcher(reader);

                // NOTE: object_field.keyword_field will be resolved to a text field for nested field as that's the default behaviour in
                // dynamic mappings too
                TermQueryBuilder termQueryBuilder = new TermQueryBuilder("object_field.keyword_field", "GET");
                termQueryBuilder.caseInsensitive(true);
                Query query = termQueryBuilder.toQuery(queryShardContext);
                TopDocs topDocs = searcher.search(query, 10);
                assertEquals(7, topDocs.totalHits.value);

                // since last doc has a malformed json, if ignore_malformed isn't set or set as false, the query should fail
                termQueryBuilder = new TermQueryBuilder("object_field_without_ignored_malformed.keyword_field", "GET");
                termQueryBuilder.caseInsensitive(true);
                query = termQueryBuilder.toQuery(queryShardContext);
                Query finalQuery = query;
                assertThrows(OpenSearchParseException.class, () -> searcher.search(finalQuery, 10));

                query = QueryBuilders.matchPhraseQuery("object_field.text_field", "document number 1").toQuery(queryShardContext);
                topDocs = searcher.search(query, 10);
                assertEquals(1, topDocs.totalHits.value);

                query = QueryBuilders.matchPhraseQuery("object_field.text_field", "document number 11").toQuery(queryShardContext);
                topDocs = searcher.search(query, 10);
                assertEquals(0, topDocs.totalHits.value);

                query = new MatchPhrasePrefixQueryBuilder("object_field.text_field", "document number").toQuery(queryShardContext);
                topDocs = searcher.search(query, 10);
                assertEquals(0, topDocs.totalHits.value);

                // Multi Phrase Query
                query = QueryBuilders.multiMatchQuery("GET", "object_field.nested_field.sub_field_1", "object_field.keyword_field")
                    .type(MultiMatchQueryBuilder.Type.PHRASE)
                    .toQuery(queryShardContext);
                topDocs = searcher.search(query, 10);
                assertEquals(7, topDocs.totalHits.value);

                // Range queries of types - date, long and double
                query = QueryBuilders.rangeQuery("object_field.date_field").from("2024-03-20T14:20:50").toQuery(queryShardContext);
                topDocs = searcher.search(query, 10);
                assertEquals(4, topDocs.totalHits.value);

                query = QueryBuilders.rangeQuery("object_field.float_field").from("4.1").toQuery(queryShardContext);
                topDocs = searcher.search(query, 10);
                assertEquals(7, topDocs.totalHits.value);

                query = QueryBuilders.rangeQuery("object_field.long_field").from("5800").toQuery(queryShardContext);
                topDocs = searcher.search(query, 10);
                assertEquals(6, topDocs.totalHits.value);

                // Prefix Query
                query = QueryBuilders.prefixQuery("object_field.keyword_field", "de").toQuery(queryShardContext);
                topDocs = searcher.search(query, 10);
                assertEquals(2, topDocs.totalHits.value);

                query = QueryBuilders.wildcardQuery("object_field.keyword_field", "g*").toQuery(queryShardContext);
                topDocs = searcher.search(query, 10);
                assertEquals(7, topDocs.totalHits.value);

                QueryStringQueryParser queryParser = new QueryStringQueryParser(queryShardContext, "object_field.keyword_field");
                queryParser.parse("GE?");
                topDocs = searcher.search(query, 10);
                assertEquals(7, topDocs.totalHits.value);

                // Regexp Query
                query = QueryBuilders.regexpQuery("object_field.keyword_field", ".*let.*").toQuery(queryShardContext);
                topDocs = searcher.search(query, 10);
                assertEquals(2, topDocs.totalHits.value);

                // tested deep nested field
                query = QueryBuilders.regexpQuery("object_field.nested_field.sub_field_1", ".*let.*").toQuery(queryShardContext);
                topDocs = searcher.search(query, 10);
                assertEquals(2, topDocs.totalHits.value);

                // Test nested array field
                query = QueryBuilders.rangeQuery("object_field.array_field").from("1").toQuery(queryShardContext);
                topDocs = searcher.search(query, 10);
                assertEquals(10, topDocs.totalHits.value);

                query = QueryBuilders.rangeQuery("object_field.array_field").from("3").to("6").toQuery(queryShardContext);
                topDocs = searcher.search(query, 10);
                assertEquals(2, topDocs.totalHits.value);

                query = QueryBuilders.rangeQuery("object_field.array_field").from("9").to("9").toQuery(queryShardContext);
                topDocs = searcher.search(query, 10);
                assertEquals(1, topDocs.totalHits.value);

                query = QueryBuilders.rangeQuery("object_field.array_field").from("10").to("12").toQuery(queryShardContext);
                topDocs = searcher.search(query, 10);
                assertEquals(1, topDocs.totalHits.value);

                query = QueryBuilders.rangeQuery("object_field.array_field").from("31").to("50").toQuery(queryShardContext);
                topDocs = searcher.search(query, 10);
                assertEquals(0, topDocs.totalHits.value);

                // tested missing nested field
                query = QueryBuilders.regexpQuery("object_field.invalid_field.sub_field", ".*let.*").toQuery(queryShardContext);
                topDocs = searcher.search(query, 10);
                assertEquals(0, topDocs.totalHits.value);
            }
        }
    }

    public void testObjectDerivedFieldsWithPrefilterField() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.startObject("properties");
            {
                b.startObject("raw_message");
                {
                    b.field("type", "text");
                }
                b.endObject();
                b.startObject("long_field");
                {
                    b.field("type", "long");
                }
                b.endObject();
            }
            b.endObject();
            b.startObject("derived");
            {
                b.startObject("regular_derived_field");
                {
                    b.field("type", "text");
                    b.field("script", "");
                    b.field("ignore_malformed", true);
                    b.field("prefilter_field", "raw_message");
                }
                b.endObject();
                b.startObject("regular_derived_field_without_prefilter_field");
                {
                    b.field("type", "text");
                    b.field("script", "");
                    b.field("ignore_malformed", true);
                }
                b.endObject();
                b.startObject("object_field");
                {
                    b.field("type", "object");
                    b.field("script", "");
                    b.field("ignore_malformed", true);
                }
                b.endObject();
                b.startObject("object_field_2");
                {
                    b.field("type", "object");
                    b.field("script", "");
                    b.startObject("properties");
                    {
                        b.field("keyword_field", "keyword");
                    }
                    b.endObject();
                    b.field("prefilter_field", "raw_message");
                    b.field("ignore_malformed", true);
                }
                b.endObject();
                b.startObject("invalid_object");
                {
                    b.field("type", "object");
                    b.field("script", "");
                    b.startObject("properties");
                    {
                        b.field("keyword_field", "keyword");
                    }
                    b.endObject();
                    b.field("prefilter_field", "invalid_field");
                    b.field("ignore_malformed", true);
                }
                b.endObject();
                b.startObject("long_prefilter_field_object");
                {
                    b.field("type", "object");
                    b.field("script", "");
                    b.startObject("properties");
                    {
                        b.field("keyword_field", "keyword");
                    }
                    b.endObject();
                    b.field("prefilter_field", "long_field");
                    b.field("ignore_malformed", true);
                }
                b.endObject();
            }
            b.endObject();
        }));

        List<Document> docs = new ArrayList<>();
        for (Object[] request : raw_requests) {
            Document document = new Document();
            document.add(new TextField("raw_message", (String) request[0], Field.Store.YES));
            docs.add(document);
        }

        int[] scriptIndex = { 8 };
        int[] docsEvaluated = { 0 };

        // Mock DerivedFieldScript.Factory
        DerivedFieldScript.Factory factory = (params, lookup) -> (DerivedFieldScript.LeafFactory) ctx -> new DerivedFieldScript(
            params,
            lookup,
            ctx
        ) {
            int docId = 0;

            @Override
            public void setDocument(int docId) {
                docsEvaluated[0]++;
                super.setDocument(docId);
                this.docId = docId;
            }

            @Override
            public void execute() {
                addEmittedValue(raw_requests[docId][scriptIndex[0]]);
            }
        };

        // Index and Search
        try (Directory dir = newDirectory()) {
            IndexWriter iw = new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER));
            for (Document d : docs) {
                iw.addDocument(d);
            }
            try (IndexReader reader = DirectoryReader.open(iw)) {
                iw.close();
                QueryShardContext queryShardContext = createQueryShardContext(mapperService);
                when(queryShardContext.compile(Mockito.any(), Mockito.any())).thenReturn(factory);
                when(queryShardContext.sourcePath("raw_message")).thenReturn(Set.of("raw_message"));
                when(queryShardContext.index()).thenReturn(new Index("test_index", "uuid"));
                when(queryShardContext.getIndexReader()).thenReturn(reader);
                when(queryShardContext.fieldMapper(anyString())).thenAnswer(inv -> {
                    MappedFieldType res = mapperService.fieldType(inv.getArguments()[0].toString());
                    if (res == null) {
                        DerivedFieldResolver derivedFieldResolver = DerivedFieldResolverFactory.createResolver(
                            queryShardContext,
                            null,
                            null,
                            true
                        );
                        res = derivedFieldResolver.resolve(inv.getArguments()[0].toString());
                    }
                    return res;
                });
                IndexSearcher searcher = new IndexSearcher(reader);

                // NOTE: object_field.keyword_field will be resolved to a text field for nested field as that's the default behaviour in
                // dynamic mappings too
                TermQueryBuilder termQueryBuilder = new TermQueryBuilder("object_field.keyword_field", "GET");
                termQueryBuilder.caseInsensitive(true);
                Query query = termQueryBuilder.toQuery(queryShardContext);
                TopDocs topDocs = searcher.search(query, 10);
                assertEquals(7, topDocs.totalHits.value);

                // test object_field_2
                docsEvaluated[0] = 0;
                termQueryBuilder = new TermQueryBuilder("object_field_2.keyword_field", "GET");
                termQueryBuilder.caseInsensitive(true);
                query = termQueryBuilder.toQuery(queryShardContext);
                topDocs = searcher.search(query, 10);
                assertEquals(7, topDocs.totalHits.value);

                // since we have prefilter_field set to "raw_message", it should not evaluate all documents
                assertEquals(8, docsEvaluated[0]);

                termQueryBuilder = new TermQueryBuilder("invalid_object.keyword_field", "GET");
                termQueryBuilder.caseInsensitive(true);
                TermQueryBuilder finalTermQueryBuilder = termQueryBuilder;
                assertThrows(MapperException.class, () -> finalTermQueryBuilder.toQuery(queryShardContext));

                termQueryBuilder = new TermQueryBuilder("long_prefilter_field_object.keyword_field", "GET");
                termQueryBuilder.caseInsensitive(true);
                TermQueryBuilder finalTermQueryBuilder2 = termQueryBuilder;
                assertThrows(MapperException.class, () -> finalTermQueryBuilder2.toQuery(queryShardContext));

                // when nested field is of numeric type or date, the range queries should still work irrespective of invalid
                // prefilter_field
                query = QueryBuilders.rangeQuery("invalid_object.date_field").from("2024-03-20T14:20:50").toQuery(queryShardContext);
                topDocs = searcher.search(query, 10);
                assertEquals(4, topDocs.totalHits.value);

                query = QueryBuilders.rangeQuery("long_prefilter_field_object.date_field")
                    .from("2024-03-20T14:20:50")
                    .toQuery(queryShardContext);
                topDocs = searcher.search(query, 10);
                assertEquals(4, topDocs.totalHits.value);

                // test regular_derived_field
                docsEvaluated[0] = 0;
                scriptIndex[0] = 4;
                query = QueryBuilders.termQuery("regular_derived_field", "delete").caseInsensitive(true).toQuery(queryShardContext);
                topDocs = searcher.search(query, 10);
                assertEquals(2, topDocs.totalHits.value);
                assertEquals(2, docsEvaluated[0]);

                // test regular_derived_field_without_prefilter_field
                docsEvaluated[0] = 0;
                scriptIndex[0] = 4;
                query = QueryBuilders.termQuery("regular_derived_field_without_prefilter_field", "delete")
                    .caseInsensitive(true)
                    .toQuery(queryShardContext);
                topDocs = searcher.search(query, 10);
                assertEquals(2, topDocs.totalHits.value);
                assertEquals(11, docsEvaluated[0]);
            }
        }
    }

    public void testObjectDerivedFieldsWithIgnoreMalformed() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.startObject("properties");
            {
                b.startObject("raw_message");
                {
                    b.field("type", "text");
                }
                b.endObject();
                b.startObject("long_field");
                {
                    b.field("type", "long");
                }
                b.endObject();
            }
            b.endObject();
            b.startObject("derived");
            {
                b.startObject("object_field");
                {
                    b.field("type", "object");
                    b.field("script", "");
                    b.field("ignore_malformed", true);
                }
                b.endObject();
                b.startObject("object_field_2");
                {
                    b.field("type", "object");
                    b.field("script", "");
                    b.startObject("properties");
                    {
                        b.field("keyword_field", "date");
                    }
                    b.endObject();
                    b.field("prefilter_field", "raw_message");
                    b.field("ignore_malformed", true);
                }
                b.endObject();
                b.startObject("object_field_3");
                {
                    b.field("type", "object");
                    b.field("script", "");
                    b.startObject("properties");
                    {
                        b.field("keyword_field", "date");
                    }
                    b.endObject();
                    b.field("prefilter_field", "raw_message");
                    b.field("ignore_malformed", false);
                }
                b.endObject();
            }
            b.endObject();
        }));

        List<Document> docs = new ArrayList<>();
        for (Object[] request : raw_requests) {
            Document document = new Document();
            document.add(new TextField("raw_message", (String) request[0], Field.Store.YES));
            docs.add(document);
        }

        int[] scriptIndex = { 8 };
        int[] docsEvaluated = { 0 };

        // Mock DerivedFieldScript.Factory
        DerivedFieldScript.Factory factory = (params, lookup) -> (DerivedFieldScript.LeafFactory) ctx -> new DerivedFieldScript(
            params,
            lookup,
            ctx
        ) {
            int docId = 0;

            @Override
            public void setDocument(int docId) {
                docsEvaluated[0]++;
                super.setDocument(docId);
                this.docId = docId;
            }

            @Override
            public void execute() {
                addEmittedValue(raw_requests[docId][scriptIndex[0]]);
            }
        };

        // Index and Search
        try (Directory dir = newDirectory()) {
            IndexWriter iw = new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER));
            for (Document d : docs) {
                iw.addDocument(d);
            }
            try (IndexReader reader = DirectoryReader.open(iw)) {
                iw.close();
                QueryShardContext queryShardContext = createQueryShardContext(mapperService);
                when(queryShardContext.compile(Mockito.any(), Mockito.any())).thenReturn(factory);
                when(queryShardContext.sourcePath("raw_message")).thenReturn(Set.of("raw_message"));
                when(queryShardContext.index()).thenReturn(new Index("test_index", "uuid"));
                when(queryShardContext.getIndexReader()).thenReturn(reader);
                when(queryShardContext.fieldMapper(anyString())).thenAnswer(inv -> {
                    MappedFieldType res = mapperService.fieldType(inv.getArguments()[0].toString());
                    if (res == null) {
                        DerivedFieldResolver derivedFieldResolver = DerivedFieldResolverFactory.createResolver(
                            queryShardContext,
                            null,
                            null,
                            true
                        );
                        res = derivedFieldResolver.resolve(inv.getArguments()[0].toString());
                    }
                    return res;
                });
                IndexSearcher searcher = new IndexSearcher(reader);

                // NOTE: object_field.keyword_field will be resolved to a text field for nested field as that's the default behaviour in
                // dynamic mappings too
                TermQueryBuilder termQueryBuilder = new TermQueryBuilder("object_field.keyword_field", "GET");
                termQueryBuilder.caseInsensitive(true);
                Query query = termQueryBuilder.toQuery(queryShardContext);
                TopDocs topDocs = searcher.search(query, 10);
                assertEquals(7, topDocs.totalHits.value);

                // check if ignoreMalformed is set to false, the query fails on malformed values
                query = QueryBuilders.rangeQuery("object_field_3.keyword_field").from("2024-03-20T14:20:50").toQuery(queryShardContext);
                Query finalQuery = query;
                assertThrows(IllegalArgumentException.class, () -> searcher.search(finalQuery, 10));

                // check if ignoreMalformed is set to true, the query passes with 0 results
                query = QueryBuilders.rangeQuery("object_field_2.keyword_field").from("2024-03-20T14:20:50").toQuery(queryShardContext);
                topDocs = searcher.search(query, 10);
                assertEquals(0, topDocs.totalHits.value);
            }
        }
    }
}
