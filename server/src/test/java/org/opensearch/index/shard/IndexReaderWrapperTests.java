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

package org.opensearch.index.shard;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.FieldFilterLeafReader;
import org.opensearch.common.CheckedFunction;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.Engine;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class IndexReaderWrapperTests extends OpenSearchTestCase {

    public void testReaderCloseListenerIsCalled() throws IOException {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig();
        IndexWriter writer = new IndexWriter(dir, iwc);
        Document doc = new Document();
        doc.add(new StringField("id", "1", random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
        doc.add(new TextField("field", "doc", random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
        writer.addDocument(doc);
        DirectoryReader open = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "_na_", 1));
        IndexSearcher searcher = new IndexSearcher(open);
        assertEquals(1, searcher.search(new TermQuery(new Term("field", "doc")), 1).totalHits.value());
        final AtomicInteger closeCalls = new AtomicInteger(0);
        CheckedFunction<DirectoryReader, DirectoryReader, IOException> wrapper = reader -> new FieldMaskingReader(
            "field",
            reader,
            closeCalls
        );
        final int sourceRefCount = open.getRefCount();
        final AtomicInteger count = new AtomicInteger();
        final AtomicInteger outerCount = new AtomicInteger();
        final AtomicBoolean closeCalled = new AtomicBoolean(false);
        final Engine.Searcher wrap = IndexShard.wrapSearcher(
            new Engine.Searcher(
                "foo",
                open,
                IndexSearcher.getDefaultSimilarity(),
                IndexSearcher.getDefaultQueryCache(),
                IndexSearcher.getDefaultQueryCachingPolicy(),
                () -> closeCalled.set(true)
            ),
            wrapper
        );
        assertEquals(1, wrap.getIndexReader().getRefCount());
        OpenSearchDirectoryReader.addReaderCloseListener(wrap.getDirectoryReader(), key -> {
            if (key == open.getReaderCacheHelper().getKey()) {
                count.incrementAndGet();
            }
            outerCount.incrementAndGet();
        });
        assertEquals(0, wrap.search(new TermQuery(new Term("field", "doc")), 1).totalHits.value());
        wrap.close();
        assertFalse("wrapped reader is closed", wrap.getIndexReader().tryIncRef());
        assertEquals(sourceRefCount, open.getRefCount());
        assertTrue(closeCalled.get());
        assertEquals(1, closeCalls.get());

        IOUtils.close(open, writer, dir);
        assertEquals(1, outerCount.get());
        assertEquals(1, count.get());
        assertEquals(0, open.getRefCount());
        assertEquals(1, closeCalls.get());
    }

    public void testIsCacheable() throws IOException {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig();
        IndexWriter writer = new IndexWriter(dir, iwc);
        Document doc = new Document();
        doc.add(new StringField("id", "1", random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
        doc.add(new TextField("field", "doc", random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
        writer.addDocument(doc);
        DirectoryReader open = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "_na_", 1));
        IndexSearcher searcher = new IndexSearcher(open);
        assertEquals(1, searcher.search(new TermQuery(new Term("field", "doc")), 1).totalHits.value());
        searcher.setSimilarity(iwc.getSimilarity());
        final AtomicInteger closeCalls = new AtomicInteger(0);
        CheckedFunction<DirectoryReader, DirectoryReader, IOException> wrapper = reader -> new FieldMaskingReader(
            "field",
            reader,
            closeCalls
        );
        final ConcurrentHashMap<Object, TopDocs> cache = new ConcurrentHashMap<>();
        AtomicBoolean closeCalled = new AtomicBoolean(false);
        try (
            Engine.Searcher wrap = IndexShard.wrapSearcher(
                new Engine.Searcher(
                    "foo",
                    open,
                    IndexSearcher.getDefaultSimilarity(),
                    IndexSearcher.getDefaultQueryCache(),
                    IndexSearcher.getDefaultQueryCachingPolicy(),
                    () -> closeCalled.set(true)
                ),
                wrapper
            )
        ) {
            OpenSearchDirectoryReader.addReaderCloseListener(wrap.getDirectoryReader(), key -> { cache.remove(key); });
            TopDocs search = wrap.search(new TermQuery(new Term("field", "doc")), 1);
            cache.put(wrap.getIndexReader().getReaderCacheHelper().getKey(), search);
        }
        assertTrue(closeCalled.get());
        assertEquals(1, closeCalls.get());

        assertEquals(1, cache.size());
        IOUtils.close(open, writer, dir);
        assertEquals(0, cache.size());
        assertEquals(1, closeCalls.get());
    }

    public void testNoWrap() throws IOException {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig();
        IndexWriter writer = new IndexWriter(dir, iwc);
        Document doc = new Document();
        doc.add(new StringField("id", "1", random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
        doc.add(new TextField("field", "doc", random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
        writer.addDocument(doc);
        DirectoryReader open = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "_na_", 1));
        IndexSearcher searcher = new IndexSearcher(open);
        assertEquals(1, searcher.search(new TermQuery(new Term("field", "doc")), 1).totalHits.value());
        searcher.setSimilarity(iwc.getSimilarity());
        CheckedFunction<DirectoryReader, DirectoryReader, IOException> wrapper = directoryReader -> directoryReader;
        try (
            Engine.Searcher engineSearcher = IndexShard.wrapSearcher(
                new Engine.Searcher(
                    "foo",
                    open,
                    IndexSearcher.getDefaultSimilarity(),
                    IndexSearcher.getDefaultQueryCache(),
                    IndexSearcher.getDefaultQueryCachingPolicy(),
                    open::close
                ),
                wrapper
            )
        ) {
            final Engine.Searcher wrap = IndexShard.wrapSearcher(engineSearcher, wrapper);
            assertSame(wrap, engineSearcher);
        }
        IOUtils.close(writer, dir);
    }

    private static class FieldMaskingReader extends FilterDirectoryReader {
        private final String field;
        private final AtomicInteger closeCalls;

        FieldMaskingReader(String field, DirectoryReader in, AtomicInteger closeCalls) throws IOException {
            super(in, new SubReaderWrapper() {
                @Override
                public LeafReader wrap(LeafReader reader) {
                    return new FieldFilterLeafReader(reader, Collections.singleton(field), true);
                }
            });
            this.closeCalls = closeCalls;
            this.field = field;
        }

        @Override
        protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
            return new FieldMaskingReader(field, in, closeCalls);
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }

        @Override
        protected void doClose() throws IOException {
            super.doClose();
            closeCalls.incrementAndGet();
        }
    }

}
