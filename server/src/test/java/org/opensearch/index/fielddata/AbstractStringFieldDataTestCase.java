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

package org.opensearch.index.fielddata;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.search.join.ToParentBlockJoinQuery;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.UnicodeUtil;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.common.lucene.search.Queries;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.opensearch.index.fielddata.fieldcomparator.BytesRefFieldComparatorSource;
import org.opensearch.index.fielddata.ordinals.GlobalOrdinalsIndexFieldData;
import org.opensearch.search.MultiValueMode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public abstract class AbstractStringFieldDataTestCase extends AbstractFieldDataImplTestCase {
    private void addField(Document d, String name, String value) {
        d.add(new StringField(name, value, Field.Store.YES));
        d.add(new SortedSetDocValuesField(name, new BytesRef(value)));
    }

    @Override
    protected void fillSingleValueAllSet() throws Exception {
        Document d = new Document();
        addField(d, "_id", "1");
        addField(d, "value", "2");
        writer.addDocument(d);

        d = new Document();
        addField(d, "_id", "1");
        addField(d, "value", "1");
        writer.addDocument(d);

        d = new Document();
        addField(d, "_id", "3");
        addField(d, "value", "3");
        writer.addDocument(d);
    }

    @Override
    protected void add2SingleValuedDocumentsAndDeleteOneOfThem() throws Exception {
        Document d = new Document();
        addField(d, "_id", "1");
        addField(d, "value", "2");
        writer.addDocument(d);

        d = new Document();
        addField(d, "_id", "2");
        addField(d, "value", "4");
        writer.addDocument(d);

        writer.commit();

        writer.deleteDocuments(new Term("_id", "1"));
    }

    @Override
    protected void fillSingleValueWithMissing() throws Exception {
        Document d = new Document();
        addField(d, "_id", "1");
        addField(d, "value", "2");
        writer.addDocument(d);

        d = new Document();
        addField(d, "_id", "2");
        // d.add(new StringField("value", one(), Field.Store.NO)); // MISSING....
        writer.addDocument(d);

        d = new Document();
        addField(d, "_id", "3");
        addField(d, "value", "3");
        writer.addDocument(d);
    }

    @Override
    protected void fillMultiValueAllSet() throws Exception {
        Document d = new Document();
        addField(d, "_id", "1");
        addField(d, "value", "2");
        addField(d, "value", "4");
        writer.addDocument(d);

        d = new Document();
        addField(d, "_id", "2");
        addField(d, "value", "1");
        writer.addDocument(d);
        writer.commit(); // TODO: Have tests with more docs for sorting

        d = new Document();
        addField(d, "_id", "3");
        addField(d, "value", "3");
        writer.addDocument(d);
    }

    @Override
    protected void fillMultiValueWithMissing() throws Exception {
        Document d = new Document();
        addField(d, "_id", "1");
        addField(d, "value", "2");
        addField(d, "value", "4");
        writer.addDocument(d);

        d = new Document();
        addField(d, "_id", "2");
        // d.add(new StringField("value", one(), Field.Store.NO)); // MISSING
        writer.addDocument(d);

        d = new Document();
        addField(d, "_id", "3");
        addField(d, "value", "3");
        writer.addDocument(d);
    }

    @Override
    protected void fillAllMissing() throws Exception {
        Document d = new Document();
        addField(d, "_id", "1");
        writer.addDocument(d);

        d = new Document();
        addField(d, "_id", "2");
        writer.addDocument(d);

        d = new Document();
        addField(d, "_id", "3");
        writer.addDocument(d);
    }

    @Override
    protected void fillExtendedMvSet() throws Exception {
        Document d = new Document();
        addField(d, "_id", "1");
        addField(d, "value", "02");
        addField(d, "value", "04");
        writer.addDocument(d);

        d = new Document();
        addField(d, "_id", "2");
        writer.addDocument(d);

        d = new Document();
        addField(d, "_id", "3");
        addField(d, "value", "03");
        writer.addDocument(d);
        writer.commit();

        d = new Document();
        addField(d, "_id", "4");
        addField(d, "value", "04");
        addField(d, "value", "05");
        addField(d, "value", "06");
        writer.addDocument(d);

        d = new Document();
        addField(d, "_id", "5");
        addField(d, "value", "06");
        addField(d, "value", "07");
        addField(d, "value", "08");
        writer.addDocument(d);

        d = new Document();
        addField(d, "_id", "6");
        writer.addDocument(d);

        d = new Document();
        addField(d, "_id", "7");
        addField(d, "value", "08");
        addField(d, "value", "09");
        addField(d, "value", "10");
        writer.addDocument(d);
        writer.commit();

        d = new Document();
        addField(d, "_id", "8");
        addField(d, "value", "!08");
        addField(d, "value", "!09");
        addField(d, "value", "!10");
        writer.addDocument(d);
    }

    public void testActualMissingValue() throws IOException {
        testActualMissingValue(false);
    }

    public void testActualMissingValueReverse() throws IOException {
        testActualMissingValue(true);
    }

    public void testActualMissingValue(boolean reverse) throws IOException {
        // missing value is set to an actual value
        final String[] values = new String[randomIntBetween(2, 30)];
        for (int i = 1; i < values.length; ++i) {
            values[i] = TestUtil.randomUnicodeString(random());
        }
        final int numDocs = scaledRandomIntBetween(10, 3072);
        for (int i = 0; i < numDocs; ++i) {
            final String value = RandomPicks.randomFrom(random(), values);
            if (value == null) {
                writer.addDocument(new Document());
            } else {
                Document d = new Document();
                addField(d, "value", value);
                writer.addDocument(d);
            }
            if (randomInt(10) == 0) {
                writer.commit();
            }
        }

        final IndexFieldData<?> indexFieldData = getForField("value");
        final String missingValue = values[1];
        IndexSearcher searcher = new IndexSearcher(DirectoryReader.open(writer));
        SortField sortField = indexFieldData.sortField(missingValue, MultiValueMode.MIN, null, reverse);
        TopFieldDocs topDocs = searcher.search(
            new MatchAllDocsQuery(),
            randomBoolean() ? numDocs : randomIntBetween(10, numDocs),
            new Sort(sortField)
        );
        assertEquals(numDocs, topDocs.totalHits.value());
        BytesRef previousValue = reverse ? UnicodeUtil.BIG_TERM : new BytesRef();
        for (int i = 0; i < topDocs.scoreDocs.length; ++i) {
            final String docValue = searcher.storedFields().document(topDocs.scoreDocs[i].doc).get("value");
            final BytesRef value = new BytesRef(docValue == null ? missingValue : docValue);
            if (reverse) {
                assertTrue(previousValue.compareTo(value) >= 0);
            } else {
                assertTrue(previousValue.compareTo(value) <= 0);
            }
            previousValue = value;
        }
        searcher.getIndexReader().close();
    }

    public void testSortMissingFirst() throws IOException {
        testSortMissing(true, false);
    }

    public void testSortMissingFirstReverse() throws IOException {
        testSortMissing(true, true);
    }

    public void testSortMissingLast() throws IOException {
        testSortMissing(false, false);
    }

    public void testSortMissingLastReverse() throws IOException {
        testSortMissing(false, true);
    }

    public void testSortMissing(boolean first, boolean reverse) throws IOException {
        final String[] values = new String[randomIntBetween(2, 10)];
        for (int i = 1; i < values.length; ++i) {
            values[i] = TestUtil.randomUnicodeString(random());
        }
        final int numDocs = scaledRandomIntBetween(10, 3072);
        for (int i = 0; i < numDocs; ++i) {
            final String value = RandomPicks.randomFrom(random(), values);
            if (value == null) {
                writer.addDocument(new Document());
            } else {
                Document d = new Document();
                addField(d, "value", value);
                writer.addDocument(d);
            }
            if (randomInt(10) == 0) {
                writer.commit();
            }
        }
        final IndexFieldData<?> indexFieldData = getForField("value");
        IndexSearcher searcher = new IndexSearcher(DirectoryReader.open(writer));
        SortField sortField = indexFieldData.sortField(first ? "_first" : "_last", MultiValueMode.MIN, null, reverse);
        TopFieldDocs topDocs = searcher.search(
            new MatchAllDocsQuery(),
            randomBoolean() ? numDocs : randomIntBetween(10, numDocs),
            new Sort(sortField)
        );
        // As of Lucene 9.0.0, totalHits may be a lower bound
        if (topDocs.totalHits.relation() == TotalHits.Relation.EQUAL_TO) {
            assertEquals(numDocs, topDocs.totalHits.value());
        } else {
            assertTrue(1000 <= topDocs.totalHits.value());
            assertTrue(numDocs >= topDocs.totalHits.value());
        }
        BytesRef previousValue = first ? null : reverse ? UnicodeUtil.BIG_TERM : new BytesRef();
        for (int i = 0; i < topDocs.scoreDocs.length; ++i) {
            final String docValue = searcher.storedFields().document(topDocs.scoreDocs[i].doc).get("value");
            if (first && docValue == null) {
                assertNull(previousValue);
            } else if (!first && docValue != null) {
                assertNotNull(previousValue);
            }
            final BytesRef value = docValue == null ? null : new BytesRef(docValue);
            if (previousValue != null && value != null) {
                if (reverse) {
                    assertTrue(previousValue.compareTo(value) >= 0);
                } else {
                    assertTrue(previousValue.compareTo(value) <= 0);
                }
            }
            previousValue = value;
        }
        searcher.getIndexReader().close();
    }

    public void testNestedSortingMin() throws IOException {
        testNestedSorting(MultiValueMode.MIN);
    }

    public void testNestedSortingMax() throws IOException {
        testNestedSorting(MultiValueMode.MAX);
    }

    public void testNestedSorting(MultiValueMode sortMode) throws IOException {
        final String[] values = new String[randomIntBetween(2, 20)];
        for (int i = 0; i < values.length; ++i) {
            values[i] = TestUtil.randomSimpleString(random());
        }
        final int numParents = scaledRandomIntBetween(10, 3072);
        List<Document> docs = new ArrayList<>();
        FixedBitSet parents = new FixedBitSet(64);
        for (int i = 0; i < numParents; ++i) {
            docs.clear();
            final int numChildren = randomInt(4);
            for (int j = 0; j < numChildren; ++j) {
                final Document child = new Document();
                final int numValues = randomInt(3);
                for (int k = 0; k < numValues; ++k) {
                    final String value = RandomPicks.randomFrom(random(), values);
                    addField(child, "text", value);
                }
                docs.add(child);
            }
            final Document parent = new Document();
            parent.add(new StringField("type", "parent", Store.YES));
            final String value = RandomPicks.randomFrom(random(), values);
            if (value != null) {
                addField(parent, "text", value);
            }
            docs.add(parent);
            int bit = parents.prevSetBit(parents.length() - 1) + docs.size();
            parents = FixedBitSet.ensureCapacity(parents, bit);
            parents.set(bit);
            writer.addDocuments(docs);
            if (randomInt(10) == 0) {
                writer.commit();
            }
        }
        DirectoryReader directoryReader = DirectoryReader.open(writer);
        directoryReader = OpenSearchDirectoryReader.wrap(directoryReader, new ShardId(indexService.index(), 0));
        IndexSearcher searcher = new IndexSearcher(directoryReader);
        IndexFieldData<?> fieldData = getForField("text");
        final Object missingValue;
        switch (randomInt(4)) {
            case 0:
                missingValue = "_first";
                break;
            case 1:
                missingValue = "_last";
                break;
            case 2:
                missingValue = new BytesRef(RandomPicks.randomFrom(random(), values));
                break;
            default:
                missingValue = new BytesRef(TestUtil.randomSimpleString(random()));
                break;
        }
        Query parentFilter = new TermQuery(new Term("type", "parent"));
        Query childFilter = Queries.not(parentFilter);
        Nested nested = createNested(searcher, parentFilter, childFilter);
        BytesRefFieldComparatorSource nestedComparatorSource = new BytesRefFieldComparatorSource(fieldData, missingValue, sortMode, nested);
        ToParentBlockJoinQuery query = new ToParentBlockJoinQuery(
            new ConstantScoreQuery(childFilter),
            new QueryBitSetProducer(parentFilter),
            ScoreMode.None
        );
        Sort sort = new Sort(new SortField("text", nestedComparatorSource));
        TopFieldDocs topDocs = searcher.search(query, randomIntBetween(1, numParents), sort);
        assertTrue(topDocs.scoreDocs.length > 0);
        BytesRef previous = null;
        for (int i = 0; i < topDocs.scoreDocs.length; ++i) {
            final int docID = topDocs.scoreDocs[i].doc;
            assertTrue("expected " + docID + " to be a parent", parents.get(docID));
            BytesRef cmpValue = null;
            for (int child = parents.prevSetBit(docID - 1) + 1; child < docID; ++child) {
                String[] sVals = searcher.storedFields().document(child).getValues("text");
                final BytesRef[] vals;
                if (sVals.length == 0) {
                    vals = new BytesRef[0];
                } else {
                    vals = new BytesRef[sVals.length];
                    for (int j = 0; j < vals.length; ++j) {
                        vals[j] = new BytesRef(sVals[j]);
                    }
                }
                for (BytesRef value : vals) {
                    if (cmpValue == null) {
                        cmpValue = value;
                    } else if (sortMode == MultiValueMode.MIN && value.compareTo(cmpValue) < 0) {
                        cmpValue = value;
                    } else if (sortMode == MultiValueMode.MAX && value.compareTo(cmpValue) > 0) {
                        cmpValue = value;
                    }
                }
            }
            if (cmpValue == null) {
                if ("_first".equals(missingValue)) {
                    cmpValue = new BytesRef();
                } else if ("_last".equals(missingValue) == false) {
                    cmpValue = (BytesRef) missingValue;
                }
            }
            if (previous != null && cmpValue != null) {
                assertTrue(previous.utf8ToString() + "   /   " + cmpValue.utf8ToString(), previous.compareTo(cmpValue) <= 0);
            }
            previous = cmpValue;
        }
        searcher.getIndexReader().close();
    }

    public void testGlobalOrdinals() throws Exception {
        fillExtendedMvSet();
        refreshReader();
        IndexOrdinalsFieldData ifd = getForField("string", "value", hasDocValues());
        IndexOrdinalsFieldData globalOrdinals = ifd.loadGlobal(topLevelReader);
        assertNotNull(globalOrdinals.getOrdinalMap());
        assertThat(topLevelReader.leaves().size(), equalTo(3));

        // First segment
        assertThat(globalOrdinals, instanceOf(GlobalOrdinalsIndexFieldData.Consumer.class));
        LeafReaderContext leaf = topLevelReader.leaves().get(0);
        LeafOrdinalsFieldData afd = globalOrdinals.load(leaf);
        SortedSetDocValues values = afd.getOrdinalsValues();
        assertTrue(values.advanceExact(0));
        long ord = values.nextOrd();
        assertThat(ord, equalTo(3L));
        assertThat(values.lookupOrd(ord).utf8ToString(), equalTo("02"));
        ord = values.nextOrd();
        assertThat(ord, equalTo(5L));
        assertThat(values.lookupOrd(ord).utf8ToString(), equalTo("04"));
        ord = values.nextOrd();
        assertThat(ord, equalTo((long) DocIdSetIterator.NO_MORE_DOCS));
        assertFalse(values.advanceExact(1));
        assertTrue(values.advanceExact(2));
        ord = values.nextOrd();
        assertThat(ord, equalTo(4L));
        assertThat(values.lookupOrd(ord).utf8ToString(), equalTo("03"));
        ord = values.nextOrd();
        assertThat(ord, equalTo((long) DocIdSetIterator.NO_MORE_DOCS));

        // Second segment
        leaf = topLevelReader.leaves().get(1);
        afd = globalOrdinals.load(leaf);
        values = afd.getOrdinalsValues();
        assertTrue(values.advanceExact(0));
        ord = values.nextOrd();
        assertThat(ord, equalTo(5L));
        assertThat(values.lookupOrd(ord).utf8ToString(), equalTo("04"));
        ord = values.nextOrd();
        assertThat(ord, equalTo(6L));
        assertThat(values.lookupOrd(ord).utf8ToString(), equalTo("05"));
        ord = values.nextOrd();
        assertThat(ord, equalTo(7L));
        assertThat(values.lookupOrd(ord).utf8ToString(), equalTo("06"));
        ord = values.nextOrd();
        assertThat(ord, equalTo((long) DocIdSetIterator.NO_MORE_DOCS));
        assertTrue(values.advanceExact(1));
        ord = values.nextOrd();
        assertThat(ord, equalTo(7L));
        assertThat(values.lookupOrd(ord).utf8ToString(), equalTo("06"));
        ord = values.nextOrd();
        assertThat(ord, equalTo(8L));
        assertThat(values.lookupOrd(ord).utf8ToString(), equalTo("07"));
        ord = values.nextOrd();
        assertThat(ord, equalTo(9L));
        assertThat(values.lookupOrd(ord).utf8ToString(), equalTo("08"));
        ord = values.nextOrd();
        assertThat(ord, equalTo((long) DocIdSetIterator.NO_MORE_DOCS));
        assertFalse(values.advanceExact(2));
        assertTrue(values.advanceExact(3));
        ord = values.nextOrd();
        assertThat(ord, equalTo(9L));
        assertThat(values.lookupOrd(ord).utf8ToString(), equalTo("08"));
        ord = values.nextOrd();
        assertThat(ord, equalTo(10L));
        assertThat(values.lookupOrd(ord).utf8ToString(), equalTo("09"));
        ord = values.nextOrd();
        assertThat(ord, equalTo(11L));
        assertThat(values.lookupOrd(ord).utf8ToString(), equalTo("10"));
        ord = values.nextOrd();
        assertThat(ord, equalTo((long) DocIdSetIterator.NO_MORE_DOCS));

        // Third segment
        leaf = topLevelReader.leaves().get(2);
        afd = globalOrdinals.load(leaf);
        values = afd.getOrdinalsValues();
        assertTrue(values.advanceExact(0));
        ord = values.nextOrd();
        assertThat(ord, equalTo(0L));
        assertThat(values.lookupOrd(ord).utf8ToString(), equalTo("!08"));
        ord = values.nextOrd();
        assertThat(ord, equalTo(1L));
        assertThat(values.lookupOrd(ord).utf8ToString(), equalTo("!09"));
        ord = values.nextOrd();
        assertThat(ord, equalTo(2L));
        assertThat(values.lookupOrd(ord).utf8ToString(), equalTo("!10"));
        ord = values.nextOrd();
        assertThat(ord, equalTo((long) DocIdSetIterator.NO_MORE_DOCS));
    }

    public void testTermsEnum() throws Exception {
        fillExtendedMvSet();
        writer.forceMerge(1);
        List<LeafReaderContext> atomicReaderContexts = refreshReader();

        IndexOrdinalsFieldData ifd = getForField("value");
        for (LeafReaderContext atomicReaderContext : atomicReaderContexts) {
            LeafOrdinalsFieldData afd = ifd.load(atomicReaderContext);

            TermsEnum termsEnum = afd.getOrdinalsValues().termsEnum();
            int size = 0;
            while (termsEnum.next() != null) {
                size++;
            }
            assertThat(size, equalTo(12));

            assertThat(termsEnum.seekExact(new BytesRef("10")), is(true));
            assertThat(termsEnum.term().utf8ToString(), equalTo("10"));
            assertThat(termsEnum.next(), nullValue());

            assertThat(termsEnum.seekExact(new BytesRef("08")), is(true));
            assertThat(termsEnum.term().utf8ToString(), equalTo("08"));
            size = 0;
            while (termsEnum.next() != null) {
                size++;
            }
            assertThat(size, equalTo(2));

            termsEnum.seekExact(8);
            assertThat(termsEnum.term().utf8ToString(), equalTo("07"));
            size = 0;
            while (termsEnum.next() != null) {
                size++;
            }
            assertThat(size, equalTo(3));
        }
    }

    public void testGlobalOrdinalsGetRemovedOnceIndexReaderCloses() throws Exception {
        fillExtendedMvSet();
        refreshReader();
        IndexOrdinalsFieldData ifd = getForField("string", "value", hasDocValues());
        IndexOrdinalsFieldData globalOrdinals = ifd.loadGlobal(topLevelReader);
        assertNotNull(globalOrdinals.getOrdinalMap());
        assertThat(ifd.loadGlobal(topLevelReader).getOrdinalMap(), sameInstance(globalOrdinals.getOrdinalMap()));
        // 3 b/c 1 segment level caches and 1 top level cache
        // in case of doc values, we don't cache atomic FD, so only the top-level cache is there
        assertThat(indicesFieldDataCache.getCache().weight(), equalTo(hasDocValues() ? 1L : 4L));

        IndexOrdinalsFieldData cachedInstance = null;
        for (Accountable ramUsage : indicesFieldDataCache.getCache().values()) {
            if (ramUsage instanceof IndexOrdinalsFieldData) {
                cachedInstance = (IndexOrdinalsFieldData) ramUsage;
                break;
            }
        }
        assertNotSame(cachedInstance, globalOrdinals);
        assertThat(cachedInstance.getOrdinalMap(), sameInstance(globalOrdinals.getOrdinalMap()));
        topLevelReader.close();
        // Now only 3 segment level entries, only the toplevel reader has been closed, but the segment readers are still used by IW
        assertThat(indicesFieldDataCache.getCache().weight(), equalTo(hasDocValues() ? 0L : 3L));

        refreshReader();
        assertThat(ifd.loadGlobal(topLevelReader), not(sameInstance(globalOrdinals)));

        indexService.clearCaches(false, true);
        assertThat(indicesFieldDataCache.getCache().weight(), equalTo(0L));
    }
}
