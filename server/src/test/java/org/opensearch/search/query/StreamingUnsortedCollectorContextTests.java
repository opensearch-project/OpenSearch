/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.store.BaseDirectoryWrapper;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.opensearch.action.support.StreamSearchChannelListener;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StreamingUnsortedCollectorContextTests extends OpenSearchTestCase {

    public void testEmitsPartialBatchWithoutFlushMode() throws Exception {
        SearchContext searchContext = mock(SearchContext.class);
        @SuppressWarnings("unchecked")
        StreamSearchChannelListener<QuerySearchResult, ?> listener = mock(StreamSearchChannelListener.class);
        when(searchContext.getStreamingBatchSize()).thenReturn(1);
        when(searchContext.getStreamChannelListener()).thenReturn((StreamSearchChannelListener) listener);

        StreamingUnsortedCollectorContext collectorContext = new StreamingUnsortedCollectorContext("test", 10, searchContext);
        Collector collector = collectorContext.create(null);

        Directory directory = newDirectory();
        if (directory instanceof BaseDirectoryWrapper) {
            ((BaseDirectoryWrapper) directory).setCheckIndexOnClose(false);
        }
        try (Directory ignored = directory; RandomIndexWriter writer = new RandomIndexWriter(random(), directory)) {
            writer.addDocument(new Document());
            try (DirectoryReader reader = writer.getReader()) {
                LeafReaderContext leaf = reader.leaves().get(0);
                LeafCollector leafCollector = collector.getLeafCollector(leaf);
                leafCollector.collect(0);
            }
        }

        verify(listener).onStreamResponse(any(QuerySearchResult.class), eq(false));
    }
}
