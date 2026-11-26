/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.opensearch.OpenSearchException;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static org.hamcrest.Matchers.instanceOf;

/**
 * Pins {@link Engine#acquireSearcher(String, Engine.SearcherSupplier)}, which {@code IndexShard} and
 * {@link Engine#acquireSearcher(String, Engine.SearcherScope, Function)} share, and the not-supported error.
 */
public class SearcherSupplierProviderTests extends OpenSearchTestCase {

    public void testAcquireSearcherTransfersSupplierOwnership() throws IOException {
        try (Directory dir = newDirectory()) {
            int docs = randomIntBetween(1, 5);
            try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
                for (int i = 0; i < docs; i++) {
                    writer.addDocument(new Document());
                }
                writer.commit();
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                AtomicBoolean innerSearcherClosed = new AtomicBoolean();
                AtomicBoolean supplierReleased = new AtomicBoolean();
                Engine.SearcherSupplier supplier = new Engine.SearcherSupplier(Function.identity()) {
                    @Override
                    protected Engine.Searcher acquireSearcherInternal(String source) {
                        return new Engine.Searcher(
                            source,
                            reader,
                            null,
                            null,
                            IndexSearcher.getDefaultQueryCachingPolicy(),
                            () -> innerSearcherClosed.set(true)
                        );
                    }

                    @Override
                    protected void doClose() {
                        supplierReleased.set(true);
                    }
                };

                Engine.Searcher searcher = Engine.acquireSearcher("test", supplier);
                assertEquals("test", searcher.source());
                assertEquals(docs, searcher.getIndexReader().numDocs());
                assertFalse("supplier must stay open while the searcher is in use", supplierReleased.get());

                searcher.close();
                assertTrue(innerSearcherClosed.get());
                assertTrue("closing the searcher must release the supplier", supplierReleased.get());
            }
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

        RuntimeException e = expectThrows(RuntimeException.class, () -> Engine.acquireSearcher("test", supplier));
        assertEquals("simulated acquire failure", e.getMessage());
        assertTrue("the supplier must not leak when searcher acquisition fails", supplierReleased.get());
    }

    public void testSearchNotSupportedIsBadRequest() {
        OpenSearchException e = SearcherSupplierProvider.searchNotSupported(new ShardId("idx", "_na_", 0), "no searcher");
        assertThat(e, instanceOf(OpenSearchStatusException.class));
        assertEquals(RestStatus.BAD_REQUEST, e.status());
        assertEquals("Shard [idx][0] does not support _search: no searcher", e.getMessage());
    }
}
