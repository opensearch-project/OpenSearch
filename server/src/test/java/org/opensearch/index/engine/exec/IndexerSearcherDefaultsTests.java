/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.opensearch.index.engine.Engine;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

/**
 * Pins the {@link Indexer} searcher default-method contract that {@link org.opensearch.index.shard.IndexShard}
 * relies on: the {@link Indexer#acquireSearcherSupplier} default throws (engine-backed indexers are dispatched
 * by the caller and must never reach it), and the {@link Indexer#acquireSearcher} default delegates to the
 * supplier with ownership of the supplier transferred to the returned searcher.
 */
public class IndexerSearcherDefaultsTests extends OpenSearchTestCase {

    /** {@code Engine.Searcher} requires a non-null caching policy; queries in these tests never cache. */
    private static final org.apache.lucene.search.QueryCachingPolicy TRIVIAL_NEVER_CACHE =
        new org.apache.lucene.search.QueryCachingPolicy() {
            @Override
            public void onUse(org.apache.lucene.search.Query query) {}

            @Override
            public boolean shouldCache(org.apache.lucene.search.Query query) {
                return false;
            }
        };

    private Indexer callsRealDefaults() {
        return mock(Indexer.class, withSettings().defaultAnswer(CALLS_REAL_METHODS));
    }

    public void testAcquireSearcherSupplierDefaultThrows() {
        Indexer indexer = callsRealDefaults();
        UnsupportedOperationException e = expectThrows(
            UnsupportedOperationException.class,
            () -> indexer.acquireSearcherSupplier(Function.identity(), Engine.SearcherScope.EXTERNAL)
        );
        assertTrue("message names the indexer class: " + e.getMessage(), e.getMessage().contains("acquireSearcherSupplier"));
    }

    public void testAcquireSearcherDefaultDelegatesAndTransfersSupplierOwnership() throws IOException {
        try (Directory dir = newDirectory()) {
            int docs = randomIntBetween(1, 5);
            try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
                for (int i = 0; i < docs; i++) {
                    writer.addDocument(new Document());
                }
                writer.commit();
            }
            DirectoryReader reader = DirectoryReader.open(dir);

            AtomicBoolean innerSearcherClosed = new AtomicBoolean();
            AtomicBoolean supplierReleased = new AtomicBoolean();
            Engine.SearcherSupplier supplier = new Engine.SearcherSupplier(Function.identity()) {
                @Override
                protected Engine.Searcher acquireSearcherInternal(String source) {
                    return new Engine.Searcher(source, reader, null, null, TRIVIAL_NEVER_CACHE, () -> innerSearcherClosed.set(true));
                }

                @Override
                protected void doClose() {
                    supplierReleased.set(true);
                }
            };

            Indexer indexer = callsRealDefaults();
            doReturn(supplier).when(indexer).acquireSearcherSupplier(any(), any());

            Engine.Searcher searcher = indexer.acquireSearcher("test", Engine.SearcherScope.EXTERNAL, Function.identity());
            assertEquals("test", searcher.source());
            assertEquals(docs, searcher.getIndexReader().numDocs());
            assertFalse("supplier must stay open while the searcher is in use", supplierReleased.get());

            searcher.close();
            assertTrue("closing the searcher must close the inner searcher", innerSearcherClosed.get());
            assertTrue("closing the searcher must release the supplier", supplierReleased.get());
            reader.close();
        }
    }

    public void testAcquireSearcherClosesSupplierWhenAcquisitionFails() {
        AtomicBoolean supplierReleased = new AtomicBoolean();
        Engine.SearcherSupplier supplier = new Engine.SearcherSupplier(Function.identity()) {
            @Override
            protected Engine.Searcher acquireSearcherInternal(String source) {
                throw new RuntimeException("simulated acquire failure");
            }

            @Override
            protected void doClose() {
                supplierReleased.set(true);
            }
        };

        Indexer indexer = callsRealDefaults();
        doReturn(supplier).when(indexer).acquireSearcherSupplier(any(), any());

        RuntimeException e = expectThrows(
            RuntimeException.class,
            () -> indexer.acquireSearcher("test", Engine.SearcherScope.EXTERNAL, Function.identity())
        );
        assertEquals("simulated acquire failure", e.getMessage());
        assertTrue("the supplier must not leak when searcher acquisition fails", supplierReleased.get());
    }
}
