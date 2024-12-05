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

package org.opensearch.index.engine;

import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene912.Lucene912Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.suggest.document.Completion912PostingsFormat;
import org.apache.lucene.search.suggest.document.SuggestField;
import org.apache.lucene.store.Directory;
import org.opensearch.OpenSearchException;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.search.suggest.completion.CompletionStats;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class CompletionStatsCacheTests extends OpenSearchTestCase {

    public void testExceptionsAreNotCached() {
        final AtomicInteger openCount = new AtomicInteger();
        final CompletionStatsCache completionStatsCache = new CompletionStatsCache(() -> {
            throw new OpenSearchException("simulated " + openCount.incrementAndGet());
        });

        assertThat(expectThrows(OpenSearchException.class, completionStatsCache::get).getMessage(), equalTo("simulated 1"));
        assertThat(expectThrows(OpenSearchException.class, completionStatsCache::get).getMessage(), equalTo("simulated 2"));
    }

    public void testCompletionStatsCache() throws IOException, InterruptedException {
        final IndexWriterConfig indexWriterConfig = newIndexWriterConfig();
        final PostingsFormat postingsFormat = new Completion912PostingsFormat();
        indexWriterConfig.setCodec(new Lucene912Codec() {
            @Override
            public PostingsFormat getPostingsFormatForField(String field) {
                return postingsFormat; // all fields are suggest fields
            }
        });

        final QueryCachingPolicy queryCachingPolicy = new QueryCachingPolicy() {
            @Override
            public void onUse(Query query) {}

            @Override
            public boolean shouldCache(Query query) {
                return false;
            }
        };

        try (Directory directory = newDirectory(); IndexWriter indexWriter = new IndexWriter(directory, indexWriterConfig)) {

            final Document document = new Document();
            document.add(new SuggestField("suggest1", "val", 1));
            document.add(new SuggestField("suggest2", "val", 1));
            document.add(new SuggestField("suggest2", "anotherval", 1));
            document.add(new SuggestField("otherfield", "val", 1));
            document.add(new SuggestField("otherfield", "anotherval", 1));
            document.add(new SuggestField("otherfield", "yetmoreval", 1));
            indexWriter.addDocument(document);

            final OpenCloseCounter openCloseCounter = new OpenCloseCounter();
            final CompletionStatsCache completionStatsCache = new CompletionStatsCache(() -> {
                openCloseCounter.countOpened();
                try {
                    final DirectoryReader directoryReader = DirectoryReader.open(indexWriter);
                    return new Engine.Searcher("test", directoryReader, null, null, queryCachingPolicy, () -> {
                        openCloseCounter.countClosed();
                        IOUtils.close(directoryReader);
                    });
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            });

            final int threadCount = 6;
            final TestHarness testHarness = new TestHarness(completionStatsCache, threadCount);
            final Thread[] threads = new Thread[threadCount];
            threads[0] = new Thread(() -> testHarness.getStats(0, "*"));
            threads[1] = new Thread(() -> testHarness.getStats(1, "suggest1", "suggest2"));
            threads[2] = new Thread(() -> testHarness.getStats(2, "sug*"));
            threads[3] = new Thread(() -> testHarness.getStats(3, "no match*"));
            threads[4] = new Thread(() -> testHarness.getStats(4));
            threads[5] = new Thread(() -> testHarness.getStats(5, (String[]) null));

            for (Thread thread : threads) {
                thread.start();
            }

            testHarness.start();

            for (Thread thread : threads) {
                thread.join();
            }

            // 0: "*" should match all fields:
            final long suggest1Size = testHarness.getResult(0).getFields().get("suggest1");
            final long suggest2Size = testHarness.getResult(0).getFields().get("suggest2");
            final long otherFieldSize = testHarness.getResult(0).getFields().get("otherfield");
            final long totalSizeInBytes = testHarness.getResult(0).getSizeInBytes();
            assertThat(suggest1Size, greaterThan(0L));
            assertThat(suggest2Size, greaterThan(0L));
            assertThat(otherFieldSize, greaterThan(0L));
            assertThat(totalSizeInBytes, equalTo(suggest1Size + suggest2Size + otherFieldSize));

            // 1: enumerating fields omits the other ones
            assertThat(testHarness.getResult(1).getSizeInBytes(), equalTo(totalSizeInBytes));
            assertThat(testHarness.getResult(1).getFields().get("suggest1"), equalTo(suggest1Size));
            assertThat(testHarness.getResult(1).getFields().get("suggest2"), equalTo(suggest2Size));
            assertFalse(testHarness.getResult(1).getFields().containsField("otherfield"));

            // 2: wildcards also exclude some fields
            assertThat(testHarness.getResult(2).getSizeInBytes(), equalTo(totalSizeInBytes));
            assertThat(testHarness.getResult(2).getFields().get("suggest1"), equalTo(suggest1Size));
            assertThat(testHarness.getResult(2).getFields().get("suggest2"), equalTo(suggest2Size));
            assertFalse(testHarness.getResult(2).getFields().containsField("otherfield"));

            // 3: non-matching wildcard returns empty set of fields
            assertThat(testHarness.getResult(3).getSizeInBytes(), equalTo(totalSizeInBytes));
            assertFalse(testHarness.getResult(3).getFields().containsField("suggest1"));
            assertFalse(testHarness.getResult(3).getFields().containsField("suggest2"));
            assertFalse(testHarness.getResult(3).getFields().containsField("otherfield"));

            // 4: no fields means per-fields stats is null
            assertThat(testHarness.getResult(4).getSizeInBytes(), equalTo(totalSizeInBytes));
            assertNull(testHarness.getResult(4).getFields());

            // 5: null fields means per-fields stats is null
            assertThat(testHarness.getResult(5).getSizeInBytes(), equalTo(totalSizeInBytes));
            assertNull(testHarness.getResult(5).getFields());

            // the stats were only computed once
            openCloseCounter.assertCount(1);

            // the stats are not recomputed on a refresh
            completionStatsCache.afterRefresh(true);
            openCloseCounter.assertCount(1);

            // but they are recomputed on the next get
            completionStatsCache.get();
            openCloseCounter.assertCount(2);

            // and they do update
            final Document document2 = new Document();
            document2.add(new SuggestField("suggest1", "foo", 1));
            document2.add(new SuggestField("suggest2", "bar", 1));
            document2.add(new SuggestField("otherfield", "baz", 1));
            indexWriter.addDocument(document2);
            completionStatsCache.afterRefresh(true);
            final CompletionStats updatedStats = completionStatsCache.get();
            assertThat(updatedStats.getSizeInBytes(), greaterThan(totalSizeInBytes));
            openCloseCounter.assertCount(3);

            // beforeRefresh does not invalidate the cache
            completionStatsCache.beforeRefresh();
            completionStatsCache.get();
            openCloseCounter.assertCount(3);

            // afterRefresh does not invalidate the cache if no refresh took place
            completionStatsCache.afterRefresh(false);
            completionStatsCache.get();
            openCloseCounter.assertCount(3);
        }
    }

    private static class OpenCloseCounter {
        private final AtomicInteger openCount = new AtomicInteger();
        private final AtomicInteger closeCount = new AtomicInteger();

        void countOpened() {
            openCount.incrementAndGet();
        }

        void countClosed() {
            closeCount.incrementAndGet();
        }

        void assertCount(int expectedCount) {
            assertThat(openCount.get(), equalTo(expectedCount));
            assertThat(closeCount.get(), equalTo(expectedCount));
        }
    }

    private static class TestHarness {
        private final CompletionStatsCache completionStatsCache;
        private final CyclicBarrier cyclicBarrier;
        private final CompletionStats[] results;

        TestHarness(CompletionStatsCache completionStatsCache, int resultCount) {
            this.completionStatsCache = completionStatsCache;
            results = new CompletionStats[resultCount];
            cyclicBarrier = new CyclicBarrier(resultCount + 1);
        }

        void getStats(int threadIndex, String... fieldPatterns) {
            start();
            results[threadIndex] = completionStatsCache.get(fieldPatterns);
        }

        void start() {
            try {
                cyclicBarrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                throw new AssertionError(e);
            }
        }

        CompletionStats getResult(int index) {
            return results[index];
        }
    }

}
