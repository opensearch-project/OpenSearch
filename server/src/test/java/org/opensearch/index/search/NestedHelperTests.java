/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.search;

import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.join.ScoreMode;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.IndexService;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.NestedPathFieldMapper;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.NestedQueryBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.io.IOException;
import java.util.Collections;

import static org.opensearch.index.mapper.FieldTypeTestCase.MOCK_QSC_ENABLE_INDEX_DOC_VALUES;

public class NestedHelperTests extends OpenSearchSingleNodeTestCase {

    IndexService indexService;
    MapperService mapperService;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("foo")
            .field("type", "keyword")
            .endObject()
            .startObject("foo2")
            .field("type", "long")
            .endObject()
            .startObject("nested1")
            .field("type", "nested")
            .startObject("properties")
            .startObject("foo")
            .field("type", "keyword")
            .endObject()
            .startObject("foo2")
            .field("type", "long")
            .endObject()
            .endObject()
            .endObject()
            .startObject("nested2")
            .field("type", "nested")
            .field("include_in_parent", true)
            .startObject("properties")
            .startObject("foo")
            .field("type", "keyword")
            .endObject()
            .startObject("foo2")
            .field("type", "long")
            .endObject()
            .endObject()
            .endObject()
            .startObject("nested3")
            .field("type", "nested")
            .field("include_in_root", true)
            .startObject("properties")
            .startObject("foo")
            .field("type", "keyword")
            .endObject()
            .startObject("foo2")
            .field("type", "long")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        indexService = createIndex("index", Settings.EMPTY, "type", mapping);
        mapperService = indexService.mapperService();
    }

    public void testMatchAll() {
        assertTrue(new NestedHelper(mapperService).mightMatchNestedDocs(new MatchAllDocsQuery()));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(new MatchAllDocsQuery(), "nested1"));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(new MatchAllDocsQuery(), "nested2"));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(new MatchAllDocsQuery(), "nested3"));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(new MatchAllDocsQuery(), "nested_missing"));
    }

    public void testMatchNo() {
        assertFalse(new NestedHelper(mapperService).mightMatchNestedDocs(new MatchNoDocsQuery()));
        assertFalse(new NestedHelper(mapperService).mightMatchNonNestedDocs(new MatchNoDocsQuery(), "nested1"));
        assertFalse(new NestedHelper(mapperService).mightMatchNonNestedDocs(new MatchNoDocsQuery(), "nested2"));
        assertFalse(new NestedHelper(mapperService).mightMatchNonNestedDocs(new MatchNoDocsQuery(), "nested3"));
        assertFalse(new NestedHelper(mapperService).mightMatchNonNestedDocs(new MatchNoDocsQuery(), "nested_missing"));
    }

    public void testTermsQuery() {
        Query termsQuery = mapperService.fieldType("foo").termsQuery(Collections.singletonList("bar"), MOCK_QSC_ENABLE_INDEX_DOC_VALUES);
        assertFalse(new NestedHelper(mapperService).mightMatchNestedDocs(termsQuery));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(termsQuery, "nested1"));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(termsQuery, "nested2"));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(termsQuery, "nested3"));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(termsQuery, "nested_missing"));

        termsQuery = mapperService.fieldType("nested1.foo").termsQuery(Collections.singletonList("bar"), MOCK_QSC_ENABLE_INDEX_DOC_VALUES);
        assertTrue(new NestedHelper(mapperService).mightMatchNestedDocs(termsQuery));
        assertFalse(new NestedHelper(mapperService).mightMatchNonNestedDocs(termsQuery, "nested1"));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(termsQuery, "nested2"));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(termsQuery, "nested3"));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(termsQuery, "nested_missing"));

        termsQuery = mapperService.fieldType("nested2.foo").termsQuery(Collections.singletonList("bar"), MOCK_QSC_ENABLE_INDEX_DOC_VALUES);
        assertTrue(new NestedHelper(mapperService).mightMatchNestedDocs(termsQuery));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(termsQuery, "nested1"));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(termsQuery, "nested2"));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(termsQuery, "nested3"));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(termsQuery, "nested_missing"));

        termsQuery = mapperService.fieldType("nested3.foo").termsQuery(Collections.singletonList("bar"), MOCK_QSC_ENABLE_INDEX_DOC_VALUES);
        assertTrue(new NestedHelper(mapperService).mightMatchNestedDocs(termsQuery));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(termsQuery, "nested1"));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(termsQuery, "nested2"));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(termsQuery, "nested3"));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(termsQuery, "nested_missing"));
    }

    public void testTermQuery() {
        Query termQuery = mapperService.fieldType("foo").termQuery("bar", null);
        assertFalse(new NestedHelper(mapperService).mightMatchNestedDocs(termQuery));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(termQuery, "nested1"));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(termQuery, "nested2"));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(termQuery, "nested3"));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(termQuery, "nested_missing"));

        termQuery = mapperService.fieldType("nested1.foo").termQuery("bar", null);
        assertTrue(new NestedHelper(mapperService).mightMatchNestedDocs(termQuery));
        assertFalse(new NestedHelper(mapperService).mightMatchNonNestedDocs(termQuery, "nested1"));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(termQuery, "nested2"));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(termQuery, "nested3"));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(termQuery, "nested_missing"));

        termQuery = mapperService.fieldType("nested2.foo").termQuery("bar", null);
        assertTrue(new NestedHelper(mapperService).mightMatchNestedDocs(termQuery));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(termQuery, "nested1"));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(termQuery, "nested2"));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(termQuery, "nested3"));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(termQuery, "nested_missing"));

        termQuery = mapperService.fieldType("nested3.foo").termQuery("bar", null);
        assertTrue(new NestedHelper(mapperService).mightMatchNestedDocs(termQuery));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(termQuery, "nested1"));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(termQuery, "nested2"));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(termQuery, "nested3"));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(termQuery, "nested_missing"));
    }

    public void testRangeQuery() {
        QueryShardContext context = createSearchContext(indexService).getQueryShardContext();
        Query rangeQuery = mapperService.fieldType("foo2").rangeQuery(2, 5, true, true, null, null, null, context);
        assertFalse(new NestedHelper(mapperService).mightMatchNestedDocs(rangeQuery));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(rangeQuery, "nested1"));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(rangeQuery, "nested2"));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(rangeQuery, "nested3"));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(rangeQuery, "nested_missing"));

        rangeQuery = mapperService.fieldType("nested1.foo2").rangeQuery(2, 5, true, true, null, null, null, context);
        assertTrue(new NestedHelper(mapperService).mightMatchNestedDocs(rangeQuery));
        assertFalse(new NestedHelper(mapperService).mightMatchNonNestedDocs(rangeQuery, "nested1"));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(rangeQuery, "nested2"));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(rangeQuery, "nested3"));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(rangeQuery, "nested_missing"));

        rangeQuery = mapperService.fieldType("nested2.foo2").rangeQuery(2, 5, true, true, null, null, null, context);
        assertTrue(new NestedHelper(mapperService).mightMatchNestedDocs(rangeQuery));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(rangeQuery, "nested1"));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(rangeQuery, "nested2"));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(rangeQuery, "nested3"));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(rangeQuery, "nested_missing"));

        rangeQuery = mapperService.fieldType("nested3.foo2").rangeQuery(2, 5, true, true, null, null, null, context);
        assertTrue(new NestedHelper(mapperService).mightMatchNestedDocs(rangeQuery));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(rangeQuery, "nested1"));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(rangeQuery, "nested2"));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(rangeQuery, "nested3"));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(rangeQuery, "nested_missing"));
    }

    public void testDisjunction() {
        BooleanQuery bq = new BooleanQuery.Builder().add(new TermQuery(new Term("foo", "bar")), Occur.SHOULD)
            .add(new TermQuery(new Term("foo", "baz")), Occur.SHOULD)
            .build();
        assertFalse(new NestedHelper(mapperService).mightMatchNestedDocs(bq));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(bq, "nested1"));

        bq = new BooleanQuery.Builder().add(new TermQuery(new Term("nested1.foo", "bar")), Occur.SHOULD)
            .add(new TermQuery(new Term("nested1.foo", "baz")), Occur.SHOULD)
            .build();
        assertTrue(new NestedHelper(mapperService).mightMatchNestedDocs(bq));
        assertFalse(new NestedHelper(mapperService).mightMatchNonNestedDocs(bq, "nested1"));

        bq = new BooleanQuery.Builder().add(new TermQuery(new Term("nested2.foo", "bar")), Occur.SHOULD)
            .add(new TermQuery(new Term("nested2.foo", "baz")), Occur.SHOULD)
            .build();
        assertTrue(new NestedHelper(mapperService).mightMatchNestedDocs(bq));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(bq, "nested2"));

        bq = new BooleanQuery.Builder().add(new TermQuery(new Term("nested3.foo", "bar")), Occur.SHOULD)
            .add(new TermQuery(new Term("nested3.foo", "baz")), Occur.SHOULD)
            .build();
        assertTrue(new NestedHelper(mapperService).mightMatchNestedDocs(bq));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(bq, "nested3"));

        bq = new BooleanQuery.Builder().add(new TermQuery(new Term("foo", "bar")), Occur.SHOULD)
            .add(new MatchAllDocsQuery(), Occur.SHOULD)
            .build();
        assertTrue(new NestedHelper(mapperService).mightMatchNestedDocs(bq));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(bq, "nested1"));

        bq = new BooleanQuery.Builder().add(new TermQuery(new Term("nested1.foo", "bar")), Occur.SHOULD)
            .add(new MatchAllDocsQuery(), Occur.SHOULD)
            .build();
        assertTrue(new NestedHelper(mapperService).mightMatchNestedDocs(bq));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(bq, "nested1"));

        bq = new BooleanQuery.Builder().add(new TermQuery(new Term("nested2.foo", "bar")), Occur.SHOULD)
            .add(new MatchAllDocsQuery(), Occur.SHOULD)
            .build();
        assertTrue(new NestedHelper(mapperService).mightMatchNestedDocs(bq));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(bq, "nested2"));

        bq = new BooleanQuery.Builder().add(new TermQuery(new Term("nested3.foo", "bar")), Occur.SHOULD)
            .add(new MatchAllDocsQuery(), Occur.SHOULD)
            .build();
        assertTrue(new NestedHelper(mapperService).mightMatchNestedDocs(bq));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(bq, "nested3"));
    }

    private static Occur requiredOccur() {
        return random().nextBoolean() ? Occur.MUST : Occur.FILTER;
    }

    public void testConjunction() {
        BooleanQuery bq = new BooleanQuery.Builder().add(new TermQuery(new Term("foo", "bar")), requiredOccur())
            .add(new MatchAllDocsQuery(), requiredOccur())
            .build();
        assertFalse(new NestedHelper(mapperService).mightMatchNestedDocs(bq));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(bq, "nested1"));

        bq = new BooleanQuery.Builder().add(new TermQuery(new Term("nested1.foo", "bar")), requiredOccur())
            .add(new MatchAllDocsQuery(), requiredOccur())
            .build();
        assertTrue(new NestedHelper(mapperService).mightMatchNestedDocs(bq));
        assertFalse(new NestedHelper(mapperService).mightMatchNonNestedDocs(bq, "nested1"));

        bq = new BooleanQuery.Builder().add(new TermQuery(new Term("nested2.foo", "bar")), requiredOccur())
            .add(new MatchAllDocsQuery(), requiredOccur())
            .build();
        assertTrue(new NestedHelper(mapperService).mightMatchNestedDocs(bq));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(bq, "nested2"));

        bq = new BooleanQuery.Builder().add(new TermQuery(new Term("nested3.foo", "bar")), requiredOccur())
            .add(new MatchAllDocsQuery(), requiredOccur())
            .build();
        assertTrue(new NestedHelper(mapperService).mightMatchNestedDocs(bq));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(bq, "nested3"));

        bq = new BooleanQuery.Builder().add(new MatchAllDocsQuery(), requiredOccur()).add(new MatchAllDocsQuery(), requiredOccur()).build();
        assertTrue(new NestedHelper(mapperService).mightMatchNestedDocs(bq));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(bq, "nested1"));

        bq = new BooleanQuery.Builder().add(new MatchAllDocsQuery(), requiredOccur()).add(new MatchAllDocsQuery(), requiredOccur()).build();
        assertTrue(new NestedHelper(mapperService).mightMatchNestedDocs(bq));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(bq, "nested1"));

        bq = new BooleanQuery.Builder().add(new MatchAllDocsQuery(), requiredOccur()).add(new MatchAllDocsQuery(), requiredOccur()).build();
        assertTrue(new NestedHelper(mapperService).mightMatchNestedDocs(bq));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(bq, "nested2"));

        bq = new BooleanQuery.Builder().add(new MatchAllDocsQuery(), requiredOccur()).add(new MatchAllDocsQuery(), requiredOccur()).build();
        assertTrue(new NestedHelper(mapperService).mightMatchNestedDocs(bq));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(bq, "nested3"));
    }

    public void testNested() throws IOException {
        QueryShardContext context = indexService.newQueryShardContext(0, new IndexSearcher(new MultiReader()), () -> 0, null);
        NestedQueryBuilder queryBuilder = new NestedQueryBuilder("nested1", new MatchAllQueryBuilder(), ScoreMode.Avg);
        OpenSearchToParentBlockJoinQuery query = (OpenSearchToParentBlockJoinQuery) queryBuilder.toQuery(context);

        Query expectedChildQuery = new BooleanQuery.Builder().add(new MatchAllDocsQuery(), Occur.MUST)
            // we automatically add a filter since the inner query might match non-nested docs
            .add(new TermQuery(new Term(NestedPathFieldMapper.NAME, "nested1")), Occur.FILTER)
            .build();
        assertEquals(expectedChildQuery, query.getChildQuery());

        assertFalse(new NestedHelper(mapperService).mightMatchNestedDocs(query));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(query, "nested1"));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(query, "nested2"));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(query, "nested3"));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(query, "nested_missing"));

        queryBuilder = new NestedQueryBuilder("nested1", new TermQueryBuilder("nested1.foo", "bar"), ScoreMode.Avg);
        query = (OpenSearchToParentBlockJoinQuery) queryBuilder.toQuery(context);

        // this time we do not add a filter since the inner query only matches inner docs
        expectedChildQuery = new TermQuery(new Term("nested1.foo", "bar"));
        assertEquals(expectedChildQuery, query.getChildQuery());

        assertFalse(new NestedHelper(mapperService).mightMatchNestedDocs(query));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(query, "nested1"));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(query, "nested2"));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(query, "nested3"));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(query, "nested_missing"));

        queryBuilder = new NestedQueryBuilder("nested2", new TermQueryBuilder("nested2.foo", "bar"), ScoreMode.Avg);
        query = (OpenSearchToParentBlockJoinQuery) queryBuilder.toQuery(context);

        // we need to add the filter again because of include_in_parent
        expectedChildQuery = new BooleanQuery.Builder().add(new TermQuery(new Term("nested2.foo", "bar")), Occur.MUST)
            .add(new TermQuery(new Term(NestedPathFieldMapper.NAME, "nested2")), Occur.FILTER)
            .build();
        assertEquals(expectedChildQuery, query.getChildQuery());

        assertFalse(new NestedHelper(mapperService).mightMatchNestedDocs(query));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(query, "nested1"));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(query, "nested2"));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(query, "nested3"));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(query, "nested_missing"));

        queryBuilder = new NestedQueryBuilder("nested3", new TermQueryBuilder("nested3.foo", "bar"), ScoreMode.Avg);
        query = (OpenSearchToParentBlockJoinQuery) queryBuilder.toQuery(context);

        // we need to add the filter again because of include_in_root
        expectedChildQuery = new BooleanQuery.Builder().add(new TermQuery(new Term("nested3.foo", "bar")), Occur.MUST)
            .add(new TermQuery(new Term(NestedPathFieldMapper.NAME, "nested3")), Occur.FILTER)
            .build();
        assertEquals(expectedChildQuery, query.getChildQuery());

        assertFalse(new NestedHelper(mapperService).mightMatchNestedDocs(query));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(query, "nested1"));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(query, "nested2"));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(query, "nested3"));
        assertTrue(new NestedHelper(mapperService).mightMatchNonNestedDocs(query, "nested_missing"));
    }
}
