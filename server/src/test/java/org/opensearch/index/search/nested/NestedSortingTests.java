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

package org.opensearch.index.search.nested;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.search.join.ToParentBlockJoinQuery;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.common.lucene.search.Queries;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.IndexService;
import org.opensearch.index.fielddata.AbstractFieldDataTestCase;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.fielddata.NoOrdinalsStringFieldDataTests;
import org.opensearch.index.fielddata.fieldcomparator.BytesRefFieldComparatorSource;
import org.opensearch.index.fielddata.plain.PagedBytesIndexFieldData;
import org.opensearch.index.mapper.NestedPathFieldMapper;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.NestedQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.MultiValueMode;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.NestedSortBuilder;
import org.opensearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.opensearch.index.mapper.SeqNoFieldMapper.PRIMARY_TERM_NAME;
import static org.hamcrest.Matchers.equalTo;

public class NestedSortingTests extends AbstractFieldDataTestCase {
    @Override
    protected String getFieldDataType() {
        return "string";
    }

    public void testDuel() throws Exception {
        final int numDocs = scaledRandomIntBetween(100, 1000);
        for (int i = 0; i < numDocs; ++i) {
            final int numChildren = randomInt(2);
            List<Document> docs = new ArrayList<>(numChildren + 1);
            for (int j = 0; j < numChildren; ++j) {
                Document doc = new Document();
                doc.add(new StringField("f", TestUtil.randomSimpleString(random(), 2), Field.Store.NO));
                doc.add(new StringField(NestedPathFieldMapper.NAME, "child", Field.Store.NO));
                docs.add(doc);
            }
            if (randomBoolean()) {
                docs.add(new Document());
            }
            Document parent = new Document();
            parent.add(new StringField(NestedPathFieldMapper.NAME, "parent", Field.Store.NO));
            docs.add(parent);
            writer.addDocuments(docs);
            if (rarely()) { // we need to have a bit more segments than what RandomIndexWriter would do by default
                DirectoryReader.open(writer).close();
            }
        }
        writer.commit();

        MultiValueMode sortMode = randomFrom(Arrays.asList(MultiValueMode.MIN, MultiValueMode.MAX));
        DirectoryReader reader = DirectoryReader.open(writer);
        reader = OpenSearchDirectoryReader.wrap(reader, new ShardId(indexService.index(), 0));
        IndexSearcher searcher = new IndexSearcher(reader);
        PagedBytesIndexFieldData indexFieldData1 = getForField("f");
        IndexFieldData<?> indexFieldData2 = NoOrdinalsStringFieldDataTests.hideOrdinals(indexFieldData1);
        final String missingValue = randomBoolean() ? null : TestUtil.randomSimpleString(random(), 2);
        final int n = randomIntBetween(1, numDocs + 2);
        final boolean reverse = randomBoolean();

        final TopDocs topDocs1 = getTopDocs(searcher, indexFieldData1, missingValue, sortMode, n, reverse);
        final TopDocs topDocs2 = getTopDocs(searcher, indexFieldData2, missingValue, sortMode, n, reverse);
        for (int i = 0; i < topDocs1.scoreDocs.length; ++i) {
            final FieldDoc fieldDoc1 = (FieldDoc) topDocs1.scoreDocs[i];
            final FieldDoc fieldDoc2 = (FieldDoc) topDocs2.scoreDocs[i];
            assertEquals(fieldDoc1.doc, fieldDoc2.doc);
            assertArrayEquals(fieldDoc1.fields, fieldDoc2.fields);
        }

        searcher.getIndexReader().close();
    }

    private TopDocs getTopDocs(
        IndexSearcher searcher,
        IndexFieldData<?> indexFieldData,
        String missingValue,
        MultiValueMode sortMode,
        int n,
        boolean reverse
    ) throws IOException {
        Query parentFilter = new TermQuery(new Term(NestedPathFieldMapper.NAME, "parent"));
        Query childFilter = new TermQuery(new Term(NestedPathFieldMapper.NAME, "child"));
        SortField sortField = indexFieldData.sortField(missingValue, sortMode, createNested(searcher, parentFilter, childFilter), reverse);
        Query query = new ConstantScoreQuery(parentFilter);
        Sort sort = new Sort(sortField);
        return searcher.search(query, n, sort);
    }

    public void testNestedSorting() throws Exception {
        List<Document> docs = new ArrayList<>();
        Document document = new Document();
        document.add(new StringField("field2", "a", Field.Store.NO));
        document.add(new StringField("filter_1", "T", Field.Store.NO));
        docs.add(document);
        document = new Document();
        document.add(new StringField("field2", "b", Field.Store.NO));
        document.add(new StringField("filter_1", "T", Field.Store.NO));
        docs.add(document);
        document = new Document();
        document.add(new StringField("field2", "c", Field.Store.NO));
        document.add(new StringField("filter_1", "T", Field.Store.NO));
        docs.add(document);
        document = new Document();
        document.add(new StringField(NestedPathFieldMapper.NAME, "parent", Field.Store.NO));
        document.add(new StringField("field1", "a", Field.Store.NO));
        docs.add(document);
        writer.addDocuments(docs);
        writer.commit();

        docs.clear();
        document = new Document();
        document.add(new StringField("field2", "c", Field.Store.NO));
        document.add(new StringField("filter_1", "T", Field.Store.NO));
        docs.add(document);
        document = new Document();
        document.add(new StringField("field2", "d", Field.Store.NO));
        document.add(new StringField("filter_1", "T", Field.Store.NO));
        docs.add(document);
        document = new Document();
        document.add(new StringField("field2", "e", Field.Store.NO));
        document.add(new StringField("filter_1", "T", Field.Store.NO));
        docs.add(document);
        document = new Document();
        document.add(new StringField(NestedPathFieldMapper.NAME, "parent", Field.Store.NO));
        document.add(new StringField("field1", "b", Field.Store.NO));
        docs.add(document);
        writer.addDocuments(docs);

        docs.clear();
        document = new Document();
        document.add(new StringField("field2", "e", Field.Store.NO));
        document.add(new StringField("filter_1", "T", Field.Store.NO));
        docs.add(document);
        document = new Document();
        document.add(new StringField("field2", "f", Field.Store.NO));
        document.add(new StringField("filter_1", "T", Field.Store.NO));
        docs.add(document);
        document = new Document();
        document.add(new StringField("field2", "g", Field.Store.NO));
        document.add(new StringField("filter_1", "T", Field.Store.NO));
        docs.add(document);
        document = new Document();
        document.add(new StringField(NestedPathFieldMapper.NAME, "parent", Field.Store.NO));
        document.add(new StringField("field1", "c", Field.Store.NO));
        docs.add(document);
        writer.addDocuments(docs);

        docs.clear();
        document = new Document();
        document.add(new StringField("field2", "g", Field.Store.NO));
        document.add(new StringField("filter_1", "T", Field.Store.NO));
        docs.add(document);
        document = new Document();
        document.add(new StringField("field2", "h", Field.Store.NO));
        document.add(new StringField("filter_1", "F", Field.Store.NO));
        docs.add(document);
        document = new Document();
        document.add(new StringField("field2", "i", Field.Store.NO));
        document.add(new StringField("filter_1", "F", Field.Store.NO));
        docs.add(document);
        document = new Document();
        document.add(new StringField(NestedPathFieldMapper.NAME, "parent", Field.Store.NO));
        document.add(new StringField("field1", "d", Field.Store.NO));
        docs.add(document);
        writer.addDocuments(docs);
        writer.commit();

        docs.clear();
        document = new Document();
        document.add(new StringField("field2", "i", Field.Store.NO));
        document.add(new StringField("filter_1", "F", Field.Store.NO));
        docs.add(document);
        document = new Document();
        document.add(new StringField("field2", "j", Field.Store.NO));
        document.add(new StringField("filter_1", "F", Field.Store.NO));
        docs.add(document);
        document = new Document();
        document.add(new StringField("field2", "k", Field.Store.NO));
        document.add(new StringField("filter_1", "F", Field.Store.NO));
        docs.add(document);
        document = new Document();
        document.add(new StringField(NestedPathFieldMapper.NAME, "parent", Field.Store.NO));
        document.add(new StringField("field1", "f", Field.Store.NO));
        docs.add(document);
        writer.addDocuments(docs);

        docs.clear();
        document = new Document();
        document.add(new StringField("field2", "k", Field.Store.NO));
        document.add(new StringField("filter_1", "T", Field.Store.NO));
        docs.add(document);
        document = new Document();
        document.add(new StringField("field2", "l", Field.Store.NO));
        document.add(new StringField("filter_1", "T", Field.Store.NO));
        docs.add(document);
        document = new Document();
        document.add(new StringField("field2", "m", Field.Store.NO));
        document.add(new StringField("filter_1", "T", Field.Store.NO));
        docs.add(document);
        document = new Document();
        document.add(new StringField(NestedPathFieldMapper.NAME, "parent", Field.Store.NO));
        document.add(new StringField("field1", "g", Field.Store.NO));
        docs.add(document);
        writer.addDocuments(docs);

        // This doc will not be included, because it doesn't have nested docs
        document = new Document();
        document.add(new StringField(NestedPathFieldMapper.NAME, "parent", Field.Store.NO));
        document.add(new StringField("field1", "h", Field.Store.NO));
        writer.addDocument(document);

        docs.clear();
        document = new Document();
        document.add(new StringField("field2", "m", Field.Store.NO));
        document.add(new StringField("filter_1", "T", Field.Store.NO));
        docs.add(document);
        document = new Document();
        document.add(new StringField("field2", "n", Field.Store.NO));
        document.add(new StringField("filter_1", "F", Field.Store.NO));
        docs.add(document);
        document = new Document();
        document.add(new StringField("field2", "o", Field.Store.NO));
        document.add(new StringField("filter_1", "F", Field.Store.NO));
        docs.add(document);
        document = new Document();
        document.add(new StringField(NestedPathFieldMapper.NAME, "parent", Field.Store.NO));
        document.add(new StringField("field1", "i", Field.Store.NO));
        docs.add(document);
        writer.addDocuments(docs);
        writer.commit();

        // Some garbage docs, just to check if the NestedFieldComparator can deal with this.
        document = new Document();
        document.add(new StringField("fieldXXX", "x", Field.Store.NO));
        writer.addDocument(document);
        document = new Document();
        document.add(new StringField("fieldXXX", "x", Field.Store.NO));
        writer.addDocument(document);
        document = new Document();
        document.add(new StringField("fieldXXX", "x", Field.Store.NO));
        writer.addDocument(document);

        MultiValueMode sortMode = MultiValueMode.MIN;
        DirectoryReader reader = DirectoryReader.open(writer);
        reader = OpenSearchDirectoryReader.wrap(reader, new ShardId(indexService.index(), 0));
        IndexSearcher searcher = new IndexSearcher(reader);
        PagedBytesIndexFieldData indexFieldData = getForField("field2");
        Query parentFilter = new TermQuery(new Term(NestedPathFieldMapper.NAME, "parent"));
        Query childFilter = Queries.not(parentFilter);
        BytesRefFieldComparatorSource nestedComparatorSource = new BytesRefFieldComparatorSource(
            indexFieldData,
            null,
            sortMode,
            createNested(searcher, parentFilter, childFilter)
        );
        ToParentBlockJoinQuery query = new ToParentBlockJoinQuery(
            new ConstantScoreQuery(childFilter),
            new QueryBitSetProducer(parentFilter),
            ScoreMode.None
        );

        Sort sort = new Sort(new SortField("field2", nestedComparatorSource));
        TopFieldDocs topDocs = searcher.search(query, 5, sort);
        assertThat(topDocs.totalHits.value(), equalTo(7L));
        assertThat(topDocs.scoreDocs.length, equalTo(5));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(3));
        assertThat(((BytesRef) ((FieldDoc) topDocs.scoreDocs[0]).fields[0]).utf8ToString(), equalTo("a"));
        assertThat(topDocs.scoreDocs[1].doc, equalTo(7));
        assertThat(((BytesRef) ((FieldDoc) topDocs.scoreDocs[1]).fields[0]).utf8ToString(), equalTo("c"));
        assertThat(topDocs.scoreDocs[2].doc, equalTo(11));
        assertThat(((BytesRef) ((FieldDoc) topDocs.scoreDocs[2]).fields[0]).utf8ToString(), equalTo("e"));
        assertThat(topDocs.scoreDocs[3].doc, equalTo(15));
        assertThat(((BytesRef) ((FieldDoc) topDocs.scoreDocs[3]).fields[0]).utf8ToString(), equalTo("g"));
        assertThat(topDocs.scoreDocs[4].doc, equalTo(19));
        assertThat(((BytesRef) ((FieldDoc) topDocs.scoreDocs[4]).fields[0]).utf8ToString(), equalTo("i"));

        sortMode = MultiValueMode.MAX;
        nestedComparatorSource = new BytesRefFieldComparatorSource(
            indexFieldData,
            null,
            sortMode,
            createNested(searcher, parentFilter, childFilter)
        );
        sort = new Sort(new SortField("field2", nestedComparatorSource, true));
        topDocs = searcher.search(query, 5, sort);
        assertThat(topDocs.totalHits.value(), equalTo(7L));
        assertThat(topDocs.scoreDocs.length, equalTo(5));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(28));
        assertThat(((BytesRef) ((FieldDoc) topDocs.scoreDocs[0]).fields[0]).utf8ToString(), equalTo("o"));
        assertThat(topDocs.scoreDocs[1].doc, equalTo(23));
        assertThat(((BytesRef) ((FieldDoc) topDocs.scoreDocs[1]).fields[0]).utf8ToString(), equalTo("m"));
        assertThat(topDocs.scoreDocs[2].doc, equalTo(19));
        assertThat(((BytesRef) ((FieldDoc) topDocs.scoreDocs[2]).fields[0]).utf8ToString(), equalTo("k"));
        assertThat(topDocs.scoreDocs[3].doc, equalTo(15));
        assertThat(((BytesRef) ((FieldDoc) topDocs.scoreDocs[3]).fields[0]).utf8ToString(), equalTo("i"));
        assertThat(topDocs.scoreDocs[4].doc, equalTo(11));
        assertThat(((BytesRef) ((FieldDoc) topDocs.scoreDocs[4]).fields[0]).utf8ToString(), equalTo("g"));

        BooleanQuery.Builder bq = new BooleanQuery.Builder();
        bq.add(parentFilter, Occur.MUST_NOT);
        bq.add(new TermQuery(new Term("filter_1", "T")), Occur.MUST);
        childFilter = bq.build();
        nestedComparatorSource = new BytesRefFieldComparatorSource(
            indexFieldData,
            null,
            sortMode,
            createNested(searcher, parentFilter, childFilter)
        );
        query = new ToParentBlockJoinQuery(new ConstantScoreQuery(childFilter), new QueryBitSetProducer(parentFilter), ScoreMode.None);
        sort = new Sort(new SortField("field2", nestedComparatorSource, true));
        topDocs = searcher.search(query, 5, sort);
        assertThat(topDocs.totalHits.value(), equalTo(6L));
        assertThat(topDocs.scoreDocs.length, equalTo(5));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(23));
        assertThat(((BytesRef) ((FieldDoc) topDocs.scoreDocs[0]).fields[0]).utf8ToString(), equalTo("m"));
        assertThat(topDocs.scoreDocs[1].doc, equalTo(28));
        assertThat(((BytesRef) ((FieldDoc) topDocs.scoreDocs[1]).fields[0]).utf8ToString(), equalTo("m"));
        assertThat(topDocs.scoreDocs[2].doc, equalTo(11));
        assertThat(((BytesRef) ((FieldDoc) topDocs.scoreDocs[2]).fields[0]).utf8ToString(), equalTo("g"));
        assertThat(topDocs.scoreDocs[3].doc, equalTo(15));
        assertThat(((BytesRef) ((FieldDoc) topDocs.scoreDocs[3]).fields[0]).utf8ToString(), equalTo("g"));
        assertThat(topDocs.scoreDocs[4].doc, equalTo(7));
        assertThat(((BytesRef) ((FieldDoc) topDocs.scoreDocs[4]).fields[0]).utf8ToString(), equalTo("e"));

        searcher.getIndexReader().close();
    }

    public void testMultiLevelNestedSorting() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder();
        mapping.startObject();
        {
            mapping.startObject("_doc");
            {
                mapping.startObject("properties");
                {
                    {
                        mapping.startObject("title");
                        mapping.field("type", "text");
                        mapping.endObject();
                    }
                    {
                        mapping.startObject("genre");
                        mapping.field("type", "keyword");
                        mapping.endObject();
                    }
                    {
                        mapping.startObject("chapters");
                        mapping.field("type", "nested");
                        {
                            mapping.startObject("properties");
                            {
                                mapping.startObject("title");
                                mapping.field("type", "text");
                                mapping.endObject();
                            }
                            {
                                mapping.startObject("read_time_seconds");
                                mapping.field("type", "integer");
                                mapping.endObject();
                            }
                            {
                                mapping.startObject("paragraphs");
                                mapping.field("type", "nested");
                                {
                                    mapping.startObject("properties");
                                    {
                                        {
                                            mapping.startObject("header");
                                            mapping.field("type", "text");
                                            mapping.endObject();
                                        }
                                        {
                                            mapping.startObject("content");
                                            mapping.field("type", "text");
                                            mapping.endObject();
                                        }
                                        {
                                            mapping.startObject("word_count");
                                            mapping.field("type", "integer");
                                            mapping.endObject();
                                        }
                                    }
                                    mapping.endObject();
                                }
                                mapping.endObject();
                            }
                            mapping.endObject();
                        }
                        mapping.endObject();
                    }
                }
                mapping.endObject();
            }
            mapping.endObject();
        }
        mapping.endObject();
        IndexService indexService = createIndex("nested_sorting", Settings.EMPTY, "_doc", mapping);

        List<List<Document>> books = new ArrayList<>();
        {
            List<Document> book = new ArrayList<>();
            Document document = new Document();
            document.add(new TextField("chapters.paragraphs.header", "Paragraph 1", Field.Store.NO));
            document.add(new StringField(NestedPathFieldMapper.NAME, "chapters.paragraphs", Field.Store.NO));
            document.add(new TextField("chapters.paragraphs.text", "some text...", Field.Store.NO));
            document.add(new SortedNumericDocValuesField("chapters.paragraphs.word_count", 743));
            document.add(new IntPoint("chapters.paragraphs.word_count", 743));
            book.add(document);
            document = new Document();
            document.add(new TextField("chapters.title", "chapter 3", Field.Store.NO));
            document.add(new StringField(NestedPathFieldMapper.NAME, "chapters", Field.Store.NO));
            document.add(new IntPoint("chapters.read_time_seconds", 400));
            document.add(new NumericDocValuesField("chapters.read_time_seconds", 400));
            book.add(document);
            document = new Document();
            document.add(new TextField("chapters.paragraphs.header", "Paragraph 1", Field.Store.NO));
            document.add(new StringField(NestedPathFieldMapper.NAME, "chapters.paragraphs", Field.Store.NO));
            document.add(new TextField("chapters.paragraphs.text", "some text...", Field.Store.NO));
            document.add(new SortedNumericDocValuesField("chapters.paragraphs.word_count", 234));
            document.add(new IntPoint("chapters.paragraphs.word_count", 234));
            book.add(document);
            document = new Document();
            document.add(new TextField("chapters.title", "chapter 2", Field.Store.NO));
            document.add(new StringField(NestedPathFieldMapper.NAME, "chapters", Field.Store.NO));
            document.add(new IntPoint("chapters.read_time_seconds", 200));
            document.add(new NumericDocValuesField("chapters.read_time_seconds", 200));
            book.add(document);
            document = new Document();
            document.add(new TextField("chapters.paragraphs.header", "Paragraph 2", Field.Store.NO));
            document.add(new StringField(NestedPathFieldMapper.NAME, "chapters.paragraphs", Field.Store.NO));
            document.add(new TextField("chapters.paragraphs.text", "some text...", Field.Store.NO));
            document.add(new SortedNumericDocValuesField("chapters.paragraphs.word_count", 478));
            document.add(new IntPoint("chapters.paragraphs.word_count", 478));
            book.add(document);
            document = new Document();
            document.add(new TextField("chapters.paragraphs.header", "Paragraph 1", Field.Store.NO));
            document.add(new StringField(NestedPathFieldMapper.NAME, "chapters.paragraphs", Field.Store.NO));
            document.add(new TextField("chapters.paragraphs.text", "some text...", Field.Store.NO));
            document.add(new SortedNumericDocValuesField("chapters.paragraphs.word_count", 849));
            document.add(new IntPoint("chapters.paragraphs.word_count", 849));
            book.add(document);
            document = new Document();
            document.add(new TextField("chapters.title", "chapter 1", Field.Store.NO));
            document.add(new StringField(NestedPathFieldMapper.NAME, "chapters", Field.Store.NO));
            document.add(new IntPoint("chapters.read_time_seconds", 1400));
            document.add(new NumericDocValuesField("chapters.read_time_seconds", 1400));
            book.add(document);
            document = new Document();
            document.add(new StringField("genre", "science fiction", Field.Store.NO));
            document.add(new StringField("_id", "1", Field.Store.YES));
            document.add(new NumericDocValuesField(PRIMARY_TERM_NAME, 0));
            book.add(document);
            books.add(book);
        }
        {
            List<Document> book = new ArrayList<>();
            Document document = new Document();
            document.add(new TextField("chapters.paragraphs.header", "Introduction", Field.Store.NO));
            document.add(new StringField(NestedPathFieldMapper.NAME, "chapters.paragraphs", Field.Store.NO));
            document.add(new TextField("chapters.paragraphs.text", "some text...", Field.Store.NO));
            document.add(new SortedNumericDocValuesField("chapters.paragraphs.word_count", 76));
            document.add(new IntPoint("chapters.paragraphs.word_count", 76));
            book.add(document);
            document = new Document();
            document.add(new TextField("chapters.title", "chapter 1", Field.Store.NO));
            document.add(new StringField(NestedPathFieldMapper.NAME, "chapters", Field.Store.NO));
            document.add(new IntPoint("chapters.read_time_seconds", 20));
            document.add(new NumericDocValuesField("chapters.read_time_seconds", 20));
            book.add(document);
            document = new Document();
            document.add(new StringField("genre", "romance", Field.Store.NO));
            document.add(new StringField("_id", "2", Field.Store.YES));
            document.add(new NumericDocValuesField(PRIMARY_TERM_NAME, 0));
            book.add(document);
            books.add(book);
        }
        {
            List<Document> book = new ArrayList<>();
            Document document = new Document();
            document.add(new TextField("chapters.paragraphs.header", "A bad dream", Field.Store.NO));
            document.add(new StringField(NestedPathFieldMapper.NAME, "chapters.paragraphs", Field.Store.NO));
            document.add(new TextField("chapters.paragraphs.text", "some text...", Field.Store.NO));
            document.add(new SortedNumericDocValuesField("chapters.paragraphs.word_count", 976));
            document.add(new IntPoint("chapters.paragraphs.word_count", 976));
            book.add(document);
            document = new Document();
            document.add(new TextField("chapters.title", "The beginning of the end", Field.Store.NO));
            document.add(new StringField(NestedPathFieldMapper.NAME, "chapters", Field.Store.NO));
            document.add(new IntPoint("chapters.read_time_seconds", 1200));
            document.add(new NumericDocValuesField("chapters.read_time_seconds", 1200));
            book.add(document);
            document = new Document();
            document.add(new StringField("genre", "horror", Field.Store.NO));
            document.add(new StringField("_id", "3", Field.Store.YES));
            document.add(new NumericDocValuesField(PRIMARY_TERM_NAME, 0));
            book.add(document);
            books.add(book);
        }
        {
            List<Document> book = new ArrayList<>();
            Document document = new Document();
            document.add(new TextField("chapters.paragraphs.header", "macaroni", Field.Store.NO));
            document.add(new StringField(NestedPathFieldMapper.NAME, "chapters.paragraphs", Field.Store.NO));
            document.add(new TextField("chapters.paragraphs.text", "some text...", Field.Store.NO));
            document.add(new SortedNumericDocValuesField("chapters.paragraphs.word_count", 180));
            document.add(new IntPoint("chapters.paragraphs.word_count", 180));
            book.add(document);
            document = new Document();
            document.add(new TextField("chapters.paragraphs.header", "hamburger", Field.Store.NO));
            document.add(new StringField(NestedPathFieldMapper.NAME, "chapters.paragraphs", Field.Store.NO));
            document.add(new TextField("chapters.paragraphs.text", "some text...", Field.Store.NO));
            document.add(new SortedNumericDocValuesField("chapters.paragraphs.word_count", 150));
            document.add(new IntPoint("chapters.paragraphs.word_count", 150));
            book.add(document);
            document = new Document();
            document.add(new TextField("chapters.paragraphs.header", "tosti", Field.Store.NO));
            document.add(new StringField(NestedPathFieldMapper.NAME, "chapters.paragraphs", Field.Store.NO));
            document.add(new TextField("chapters.paragraphs.text", "some text...", Field.Store.NO));
            document.add(new SortedNumericDocValuesField("chapters.paragraphs.word_count", 120));
            document.add(new IntPoint("chapters.paragraphs.word_count", 120));
            book.add(document);
            document = new Document();
            document.add(new TextField("chapters.title", "easy meals", Field.Store.NO));
            document.add(new StringField(NestedPathFieldMapper.NAME, "chapters", Field.Store.NO));
            document.add(new IntPoint("chapters.read_time_seconds", 800));
            document.add(new NumericDocValuesField("chapters.read_time_seconds", 800));
            book.add(document);
            document = new Document();
            document.add(new TextField("chapters.paragraphs.header", "introduction", Field.Store.NO));
            document.add(new StringField(NestedPathFieldMapper.NAME, "chapters.paragraphs", Field.Store.NO));
            document.add(new TextField("chapters.paragraphs.text", "some text...", Field.Store.NO));
            document.add(new SortedNumericDocValuesField("chapters.paragraphs.word_count", 87));
            document.add(new IntPoint("chapters.paragraphs.word_count", 87));
            book.add(document);
            document = new Document();
            document.add(new TextField("chapters.title", "introduction", Field.Store.NO));
            document.add(new StringField(NestedPathFieldMapper.NAME, "chapters", Field.Store.NO));
            document.add(new IntPoint("chapters.read_time_seconds", 10));
            document.add(new NumericDocValuesField("chapters.read_time_seconds", 10));
            book.add(document);
            document = new Document();
            document.add(new StringField("genre", "cooking", Field.Store.NO));
            document.add(new StringField("_id", "4", Field.Store.YES));
            document.add(new NumericDocValuesField(PRIMARY_TERM_NAME, 0));
            book.add(document);
            books.add(book);
        }
        {
            List<Document> book = new ArrayList<>();
            Document document = new Document();
            document.add(new StringField("genre", "unknown", Field.Store.NO));
            document.add(new StringField("_id", "5", Field.Store.YES));
            document.add(new NumericDocValuesField(PRIMARY_TERM_NAME, 0));
            book.add(document);
            books.add(book);
        }

        Collections.shuffle(books, random());
        for (List<Document> book : books) {
            writer.addDocuments(book);
            if (randomBoolean()) {
                writer.commit();
            }
        }
        DirectoryReader reader = DirectoryReader.open(writer);
        reader = OpenSearchDirectoryReader.wrap(reader, new ShardId(indexService.index(), 0));
        IndexSearcher searcher = new IndexSearcher(reader);
        QueryShardContext queryShardContext = indexService.newQueryShardContext(0, searcher, () -> 0L, null);

        FieldSortBuilder sortBuilder = new FieldSortBuilder("chapters.paragraphs.word_count");
        sortBuilder.setNestedSort(new NestedSortBuilder("chapters").setNestedSort(new NestedSortBuilder("chapters.paragraphs")));
        QueryBuilder queryBuilder = new MatchAllQueryBuilder();
        TopFieldDocs topFields = search(queryBuilder, sortBuilder, queryShardContext, searcher);
        assertThat(topFields.totalHits.value(), equalTo(5L));
        assertThat(searcher.storedFields().document(topFields.scoreDocs[0].doc).get("_id"), equalTo("2"));
        assertThat(((FieldDoc) topFields.scoreDocs[0]).fields[0], equalTo(76));
        assertThat(searcher.storedFields().document(topFields.scoreDocs[1].doc).get("_id"), equalTo("4"));
        assertThat(((FieldDoc) topFields.scoreDocs[1]).fields[0], equalTo(87));
        assertThat(searcher.storedFields().document(topFields.scoreDocs[2].doc).get("_id"), equalTo("1"));
        assertThat(((FieldDoc) topFields.scoreDocs[2]).fields[0], equalTo(234));
        assertThat(searcher.storedFields().document(topFields.scoreDocs[3].doc).get("_id"), equalTo("3"));
        assertThat(((FieldDoc) topFields.scoreDocs[3]).fields[0], equalTo(976));
        assertThat(searcher.storedFields().document(topFields.scoreDocs[4].doc).get("_id"), equalTo("5"));
        assertThat(((FieldDoc) topFields.scoreDocs[4]).fields[0], equalTo(Integer.MAX_VALUE));

        // Specific genre
        {
            queryBuilder = new TermQueryBuilder("genre", "romance");
            topFields = search(queryBuilder, sortBuilder, queryShardContext, searcher);
            assertThat(topFields.totalHits.value(), equalTo(1L));
            assertThat(searcher.storedFields().document(topFields.scoreDocs[0].doc).get("_id"), equalTo("2"));
            assertThat(((FieldDoc) topFields.scoreDocs[0]).fields[0], equalTo(76));

            queryBuilder = new TermQueryBuilder("genre", "science fiction");
            topFields = search(queryBuilder, sortBuilder, queryShardContext, searcher);
            assertThat(topFields.totalHits.value(), equalTo(1L));
            assertThat(searcher.storedFields().document(topFields.scoreDocs[0].doc).get("_id"), equalTo("1"));
            assertThat(((FieldDoc) topFields.scoreDocs[0]).fields[0], equalTo(234));

            queryBuilder = new TermQueryBuilder("genre", "horror");
            topFields = search(queryBuilder, sortBuilder, queryShardContext, searcher);
            assertThat(topFields.totalHits.value(), equalTo(1L));
            assertThat(searcher.storedFields().document(topFields.scoreDocs[0].doc).get("_id"), equalTo("3"));
            assertThat(((FieldDoc) topFields.scoreDocs[0]).fields[0], equalTo(976));

            queryBuilder = new TermQueryBuilder("genre", "cooking");
            topFields = search(queryBuilder, sortBuilder, queryShardContext, searcher);
            assertThat(topFields.totalHits.value(), equalTo(1L));
            assertThat(searcher.storedFields().document(topFields.scoreDocs[0].doc).get("_id"), equalTo("4"));
            assertThat(((FieldDoc) topFields.scoreDocs[0]).fields[0], equalTo(87));
        }

        // reverse sort order
        {
            sortBuilder.order(SortOrder.DESC);
            queryBuilder = new MatchAllQueryBuilder();
            topFields = search(queryBuilder, sortBuilder, queryShardContext, searcher);
            assertThat(topFields.totalHits.value(), equalTo(5L));
            assertThat(searcher.storedFields().document(topFields.scoreDocs[0].doc).get("_id"), equalTo("3"));
            assertThat(((FieldDoc) topFields.scoreDocs[0]).fields[0], equalTo(976));
            assertThat(searcher.storedFields().document(topFields.scoreDocs[1].doc).get("_id"), equalTo("1"));
            assertThat(((FieldDoc) topFields.scoreDocs[1]).fields[0], equalTo(849));
            assertThat(searcher.storedFields().document(topFields.scoreDocs[2].doc).get("_id"), equalTo("4"));
            assertThat(((FieldDoc) topFields.scoreDocs[2]).fields[0], equalTo(180));
            assertThat(searcher.storedFields().document(topFields.scoreDocs[3].doc).get("_id"), equalTo("2"));
            assertThat(((FieldDoc) topFields.scoreDocs[3]).fields[0], equalTo(76));
            assertThat(searcher.storedFields().document(topFields.scoreDocs[4].doc).get("_id"), equalTo("5"));
            assertThat(((FieldDoc) topFields.scoreDocs[4]).fields[0], equalTo(Integer.MIN_VALUE));
        }

        // Specific genre and reverse sort order
        {
            queryBuilder = new TermQueryBuilder("genre", "romance");
            topFields = search(queryBuilder, sortBuilder, queryShardContext, searcher);
            assertThat(topFields.totalHits.value(), equalTo(1L));
            assertThat(searcher.storedFields().document(topFields.scoreDocs[0].doc).get("_id"), equalTo("2"));
            assertThat(((FieldDoc) topFields.scoreDocs[0]).fields[0], equalTo(76));

            queryBuilder = new TermQueryBuilder("genre", "science fiction");
            topFields = search(queryBuilder, sortBuilder, queryShardContext, searcher);
            assertThat(topFields.totalHits.value(), equalTo(1L));
            assertThat(searcher.storedFields().document(topFields.scoreDocs[0].doc).get("_id"), equalTo("1"));
            assertThat(((FieldDoc) topFields.scoreDocs[0]).fields[0], equalTo(849));

            queryBuilder = new TermQueryBuilder("genre", "horror");
            topFields = search(queryBuilder, sortBuilder, queryShardContext, searcher);
            assertThat(topFields.totalHits.value(), equalTo(1L));
            assertThat(searcher.storedFields().document(topFields.scoreDocs[0].doc).get("_id"), equalTo("3"));
            assertThat(((FieldDoc) topFields.scoreDocs[0]).fields[0], equalTo(976));

            queryBuilder = new TermQueryBuilder("genre", "cooking");
            topFields = search(queryBuilder, sortBuilder, queryShardContext, searcher);
            assertThat(topFields.totalHits.value(), equalTo(1L));
            assertThat(searcher.storedFields().document(topFields.scoreDocs[0].doc).get("_id"), equalTo("4"));
            assertThat(((FieldDoc) topFields.scoreDocs[0]).fields[0], equalTo(180));
        }

        // Nested filter + query
        {
            queryBuilder = new RangeQueryBuilder("chapters.read_time_seconds").to(50L);
            sortBuilder = new FieldSortBuilder("chapters.paragraphs.word_count");
            sortBuilder.setNestedSort(
                new NestedSortBuilder("chapters").setFilter(queryBuilder).setNestedSort(new NestedSortBuilder("chapters.paragraphs"))
            );
            topFields = search(new NestedQueryBuilder("chapters", queryBuilder, ScoreMode.None), sortBuilder, queryShardContext, searcher);
            assertThat(topFields.totalHits.value(), equalTo(2L));
            assertThat(searcher.storedFields().document(topFields.scoreDocs[0].doc).get("_id"), equalTo("2"));
            assertThat(((FieldDoc) topFields.scoreDocs[0]).fields[0], equalTo(76));
            assertThat(searcher.storedFields().document(topFields.scoreDocs[1].doc).get("_id"), equalTo("4"));
            assertThat(((FieldDoc) topFields.scoreDocs[1]).fields[0], equalTo(87));

            sortBuilder.order(SortOrder.DESC);
            topFields = search(new NestedQueryBuilder("chapters", queryBuilder, ScoreMode.None), sortBuilder, queryShardContext, searcher);
            assertThat(topFields.totalHits.value(), equalTo(2L));
            assertThat(searcher.storedFields().document(topFields.scoreDocs[0].doc).get("_id"), equalTo("4"));
            assertThat(((FieldDoc) topFields.scoreDocs[0]).fields[0], equalTo(87));
            assertThat(searcher.storedFields().document(topFields.scoreDocs[1].doc).get("_id"), equalTo("2"));
            assertThat(((FieldDoc) topFields.scoreDocs[1]).fields[0], equalTo(76));
        }

        // Multiple Nested filters + query
        {
            queryBuilder = new RangeQueryBuilder("chapters.read_time_seconds").to(50L);
            sortBuilder = new FieldSortBuilder("chapters.paragraphs.word_count");
            sortBuilder.setNestedSort(
                new NestedSortBuilder("chapters").setFilter(queryBuilder)
                    .setNestedSort(
                        new NestedSortBuilder("chapters.paragraphs").setFilter(
                            new RangeQueryBuilder("chapters.paragraphs.word_count").from(80L)
                        )
                    )
            );
            topFields = search(new NestedQueryBuilder("chapters", queryBuilder, ScoreMode.None), sortBuilder, queryShardContext, searcher);
            assertThat(topFields.totalHits.value(), equalTo(2L));
            assertThat(searcher.storedFields().document(topFields.scoreDocs[0].doc).get("_id"), equalTo("4"));
            assertThat(((FieldDoc) topFields.scoreDocs[0]).fields[0], equalTo(87));
            assertThat(searcher.storedFields().document(topFields.scoreDocs[1].doc).get("_id"), equalTo("2"));
            assertThat(((FieldDoc) topFields.scoreDocs[1]).fields[0], equalTo(Integer.MAX_VALUE));

            sortBuilder.order(SortOrder.DESC);
            topFields = search(new NestedQueryBuilder("chapters", queryBuilder, ScoreMode.None), sortBuilder, queryShardContext, searcher);
            assertThat(topFields.totalHits.value(), equalTo(2L));
            assertThat(searcher.storedFields().document(topFields.scoreDocs[0].doc).get("_id"), equalTo("4"));
            assertThat(((FieldDoc) topFields.scoreDocs[0]).fields[0], equalTo(87));
            assertThat(searcher.storedFields().document(topFields.scoreDocs[1].doc).get("_id"), equalTo("2"));
            assertThat(((FieldDoc) topFields.scoreDocs[1]).fields[0], equalTo(Integer.MIN_VALUE));
        }

        // Nested filter + Specific genre
        {
            sortBuilder = new FieldSortBuilder("chapters.paragraphs.word_count");
            sortBuilder.setNestedSort(
                new NestedSortBuilder("chapters").setFilter(new RangeQueryBuilder("chapters.read_time_seconds").to(50L))
                    .setNestedSort(new NestedSortBuilder("chapters.paragraphs"))
            );

            queryBuilder = new TermQueryBuilder("genre", "romance");
            topFields = search(queryBuilder, sortBuilder, queryShardContext, searcher);
            assertThat(topFields.totalHits.value(), equalTo(1L));
            assertThat(searcher.storedFields().document(topFields.scoreDocs[0].doc).get("_id"), equalTo("2"));
            assertThat(((FieldDoc) topFields.scoreDocs[0]).fields[0], equalTo(76));

            queryBuilder = new TermQueryBuilder("genre", "science fiction");
            topFields = search(queryBuilder, sortBuilder, queryShardContext, searcher);
            assertThat(topFields.totalHits.value(), equalTo(1L));
            assertThat(searcher.storedFields().document(topFields.scoreDocs[0].doc).get("_id"), equalTo("1"));
            assertThat(((FieldDoc) topFields.scoreDocs[0]).fields[0], equalTo(Integer.MAX_VALUE));

            queryBuilder = new TermQueryBuilder("genre", "horror");
            topFields = search(queryBuilder, sortBuilder, queryShardContext, searcher);
            assertThat(topFields.totalHits.value(), equalTo(1L));
            assertThat(searcher.storedFields().document(topFields.scoreDocs[0].doc).get("_id"), equalTo("3"));
            assertThat(((FieldDoc) topFields.scoreDocs[0]).fields[0], equalTo(Integer.MAX_VALUE));

            queryBuilder = new TermQueryBuilder("genre", "cooking");
            topFields = search(queryBuilder, sortBuilder, queryShardContext, searcher);
            assertThat(topFields.totalHits.value(), equalTo(1L));
            assertThat(searcher.storedFields().document(topFields.scoreDocs[0].doc).get("_id"), equalTo("4"));
            assertThat(((FieldDoc) topFields.scoreDocs[0]).fields[0], equalTo(87));
        }

        searcher.getIndexReader().close();
    }

    private static TopFieldDocs search(
        QueryBuilder queryBuilder,
        FieldSortBuilder sortBuilder,
        QueryShardContext queryShardContext,
        IndexSearcher searcher
    ) throws IOException {
        Query query = new BooleanQuery.Builder().add(queryBuilder.toQuery(queryShardContext), Occur.MUST)
            .add(Queries.newNonNestedFilter(), Occur.FILTER)
            .build();
        Sort sort = new Sort(sortBuilder.build(queryShardContext).field);
        return searcher.search(query, 10, sort);
    }

}
