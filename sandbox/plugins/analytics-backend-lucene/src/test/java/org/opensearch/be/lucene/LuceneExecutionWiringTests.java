/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import net.jqwik.api.Example;

import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.opensearch.analytics.exec.DefaultShardExecutionContext;
import org.opensearch.be.lucene.predicate.QueryBuilderSerializer;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.core.index.shard.ShardId;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the Lucene query execution wiring via initialize/execute/close lifecycle.
 */
class LuceneExecutionWiringTests {

    // --- Helper: in-memory Lucene index ---

    private static class InMemoryIndex implements AutoCloseable {
        final Directory directory;
        final DirectoryReader reader;

        InMemoryIndex(String fieldName, String... values) throws IOException {
            directory = new ByteBuffersDirectory();
            IndexWriterConfig config = new IndexWriterConfig();
            try (IndexWriter writer = new IndexWriter(directory, config)) {
                for (String value : values) {
                    Document doc = new Document();
                    doc.add(new KeywordField(fieldName, value, Field.Store.NO));
                    writer.addDocument(doc);
                }
                writer.commit();
            }
            reader = DirectoryReader.open(directory);
        }

        @Override
        public void close() throws IOException {
            reader.close();
            directory.close();
        }
    }

    // --- Helper: create Engine.Searcher wrapping a DirectoryReader ---

    private static Engine.Searcher createEngineSearcher(DirectoryReader reader, AtomicBoolean closed) {
        return new Engine.Searcher(
            "lucene-analytics",
            reader,
            new BM25Similarity(),
            null,
            mock(QueryCachingPolicy.class),
            () -> closed.set(true)
        );
    }

    // --- Helper: create mocked DefaultShardExecutionContext ---

    private static DefaultShardExecutionContext createMockShardContext(
        DirectoryReader reader,
        AtomicBoolean searcherClosed,
        String fieldName
    ) {
        // Mock QueryShardContext with keyword field mapping
        QueryShardContext qsc = mock(QueryShardContext.class);
        KeywordFieldMapper.KeywordFieldType keywordFieldType =
            new KeywordFieldMapper.KeywordFieldType(fieldName);
        when(qsc.fieldMapper(fieldName)).thenReturn(keywordFieldType);

        // Mock IndexShard to return our Engine.Searcher
        IndexShard mockShard = mock(IndexShard.class);
        when(mockShard.acquireSearcher("lucene-analytics"))
            .thenReturn(createEngineSearcher(reader, searcherClosed));
        when(mockShard.shardId()).thenReturn(new ShardId("test-index", "_na_", 0));

        // Mock IndexService to return our QueryShardContext
        IndexService mockIndexService = mock(IndexService.class);
        when(mockIndexService.newQueryShardContext(anyInt(), any(IndexSearcher.class), any(), nullable(String.class)))
            .thenReturn(qsc);

        return new DefaultShardExecutionContext(mockShard, mockIndexService);
    }

    // --- Test: execute without initialize returns empty result ---

    @Example
    void executeWithoutInitializeReturnsEmptyResult() {
        LuceneFilterExecutor bridge = new LuceneFilterExecutor();
        try {
            byte[] serialized = QueryBuilderSerializer.serialize(new TermQueryBuilder("verb", "GET"));

            Iterator<VectorSchemaRoot> results = bridge.execute(serialized);

            assertThat(results.hasNext()).isTrue();
            VectorSchemaRoot root = results.next();
            assertThat(root.getRowCount()).isEqualTo(0);
            root.close();
        } finally {
            bridge.close();
        }
    }

    // --- Test: matching doc IDs appear as set bits in BitVector ---

    @Example
    void executeAfterInitializeReturnsMatchingDocIds() throws IOException {
        try (InMemoryIndex idx = new InMemoryIndex("verb", "GET", "POST", "GET", "PUT")) {
            AtomicBoolean searcherClosed = new AtomicBoolean(false);
            DefaultShardExecutionContext ctx = createMockShardContext(idx.reader, searcherClosed, "verb");

            LuceneFilterExecutor bridge = new LuceneFilterExecutor();
            bridge.initialize(ctx);
            try {
                byte[] serialized = QueryBuilderSerializer.serialize(new TermQueryBuilder("verb", "GET"));
                Iterator<VectorSchemaRoot> results = bridge.execute(serialized);

                assertThat(results.hasNext()).isTrue();
                VectorSchemaRoot root = results.next();
                assertThat(root.getRowCount()).isEqualTo(4);

                BitVector docIds = (BitVector) root.getVector(LuceneFilterExecutor.DOC_IDS_COLUMN);
                assertThat(docIds.get(0)).isEqualTo(1); // GET
                assertThat(docIds.get(1)).isEqualTo(0); // POST
                assertThat(docIds.get(2)).isEqualTo(1); // GET
                assertThat(docIds.get(3)).isEqualTo(0); // PUT
                root.close();
            } finally {
                bridge.close();
            }
            assertThat(searcherClosed.get()).isTrue();
        }
    }

    // --- Test: zero-match query returns all-zeros BitVector ---

    @Example
    void executeWithZeroMatchReturnsAllZeroBitVector() throws IOException {
        try (InMemoryIndex idx = new InMemoryIndex("verb", "GET", "POST", "PUT")) {
            AtomicBoolean searcherClosed = new AtomicBoolean(false);
            DefaultShardExecutionContext ctx = createMockShardContext(idx.reader, searcherClosed, "verb");

            LuceneFilterExecutor bridge = new LuceneFilterExecutor();
            bridge.initialize(ctx);
            try {
                byte[] serialized = QueryBuilderSerializer.serialize(new TermQueryBuilder("verb", "DELETE"));
                Iterator<VectorSchemaRoot> results = bridge.execute(serialized);

                VectorSchemaRoot root = results.next();
                assertThat(root.getRowCount()).isEqualTo(3);
                BitVector docIds = (BitVector) root.getVector(LuceneFilterExecutor.DOC_IDS_COLUMN);
                for (int i = 0; i < 3; i++) {
                    assertThat(docIds.get(i)).as("doc %d should not match", i).isEqualTo(0);
                }
                root.close();
            } finally {
                bridge.close();
            }
            assertThat(searcherClosed.get()).isTrue();
        }
    }

    // --- Test: close releases the Engine.Searcher ---

    @Example
    void closeReleasesEngineSearcher() throws IOException {
        try (InMemoryIndex idx = new InMemoryIndex("verb", "GET")) {
            AtomicBoolean searcherClosed = new AtomicBoolean(false);
            DefaultShardExecutionContext ctx = createMockShardContext(idx.reader, searcherClosed, "verb");

            LuceneFilterExecutor bridge = new LuceneFilterExecutor();
            bridge.initialize(ctx);
            assertThat(searcherClosed.get()).isFalse();

            bridge.close();
            assertThat(searcherClosed.get())
                .as("Engine.Searcher should be closed after bridge.close()")
                .isTrue();
        }
    }

    // --- Test: all documents match returns all-ones BitVector ---

    @Example
    void executeWithAllMatchReturnsAllOnesBitVector() throws IOException {
        try (InMemoryIndex idx = new InMemoryIndex("verb", "GET", "GET", "GET")) {
            AtomicBoolean searcherClosed = new AtomicBoolean(false);
            DefaultShardExecutionContext ctx = createMockShardContext(idx.reader, searcherClosed, "verb");

            LuceneFilterExecutor bridge = new LuceneFilterExecutor();
            bridge.initialize(ctx);
            try {
                byte[] serialized = QueryBuilderSerializer.serialize(new TermQueryBuilder("verb", "GET"));
                Iterator<VectorSchemaRoot> results = bridge.execute(serialized);

                VectorSchemaRoot root = results.next();
                assertThat(root.getRowCount()).isEqualTo(3);
                BitVector docIds = (BitVector) root.getVector(LuceneFilterExecutor.DOC_IDS_COLUMN);
                for (int i = 0; i < 3; i++) {
                    assertThat(docIds.get(i)).as("doc %d should match", i).isEqualTo(1);
                }
                root.close();
            } finally {
                bridge.close();
            }
        }
    }
}
