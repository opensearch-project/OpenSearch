/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.RefreshInput;
import org.opensearch.index.engine.dataformat.RefreshResult;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.commit.CommitterSettings;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Tests for {@link LuceneIndexingExecutionEngine}.
 */
public class LuceneIndexingExecutionEngineTests extends OpenSearchTestCase {

    private LuceneCommitter committer;

    private LuceneCommitter createAndInitCommitter() throws IOException {
        Path baseDir = createTempDir();
        ShardId shardId = new ShardId("test", "_na_", 0);
        Path dataPath = baseDir.resolve(shardId.getIndex().getUUID()).resolve(Integer.toString(shardId.id()));
        Files.createDirectories(dataPath);
        ShardPath shardPath = new ShardPath(false, dataPath, dataPath, shardId);
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", Settings.EMPTY);

        LuceneCommitter c = new LuceneCommitter();
        c.init(new CommitterSettings(shardPath, indexSettings));
        return c;
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        committer = createAndInitCommitter();
    }

    @Override
    public void tearDown() throws Exception {
        if (committer != null) {
            committer.close();
        }
        super.tearDown();
    }

    /**
     * Property 1: refresh incorporates Lucene segments when parent writer is present.
     * Validates: Requirements 2.3
     */
    public void testRefreshIncorporatesLuceneSegments() throws IOException {
        LuceneIndexingExecutionEngine engine = new LuceneIndexingExecutionEngine(committer.getIndexWriter());
        IndexWriter writer = committer.getIndexWriter();
        assertEquals(0, writer.getDocStats().numDocs);

        int numDocs = randomIntBetween(1, 20);
        // Directory must end with the data format name ("lucene") for the engine to recognize it
        Path externalDir = createTempDir().resolve("lucene");
        Files.createDirectories(externalDir);
        try (
            Directory extDirectory = FSDirectory.open(externalDir);
            IndexWriter extWriter = new IndexWriter(extDirectory, new IndexWriterConfig())
        ) {
            for (int i = 0; i < numDocs; i++) {
                Document doc = new Document();
                doc.add(new StringField("id", "doc_" + i, Field.Store.YES));
                extWriter.addDocument(doc);
            }
            extWriter.commit();
        }

        WriterFileSet writerFileSet = WriterFileSet.builder().directory(externalDir).writerGeneration(1L).addNumRows(numDocs).build();
        RefreshInput refreshInput = RefreshInput.builder().addWriterFileSet(writerFileSet).build();

        RefreshResult result = engine.refresh(refreshInput);
        assertNotNull(result);
        assertEquals(numDocs, writer.getDocStats().numDocs);
    }

    /**
     * Refresh skips WriterFileSets whose directory does not match the Lucene data format name.
     */
    public void testRefreshSkipsNonLuceneDirectories() throws IOException {
        LuceneIndexingExecutionEngine engine = new LuceneIndexingExecutionEngine(committer.getIndexWriter());
        IndexWriter writer = committer.getIndexWriter();

        // Create a directory named "parquet" — should be skipped by the Lucene engine
        Path parquetDir = createTempDir().resolve("parquet");
        Files.createDirectories(parquetDir);
        try (
            Directory extDirectory = FSDirectory.open(parquetDir);
            IndexWriter extWriter = new IndexWriter(extDirectory, new IndexWriterConfig())
        ) {
            Document doc = new Document();
            doc.add(new StringField("id", "doc_0", Field.Store.YES));
            extWriter.addDocument(doc);
            extWriter.commit();
        }

        WriterFileSet writerFileSet = WriterFileSet.builder().directory(parquetDir).writerGeneration(1L).addNumRows(1).build();
        RefreshInput refreshInput = RefreshInput.builder().addWriterFileSet(writerFileSet).build();

        engine.refresh(refreshInput);
        assertEquals("Non-lucene directories should be skipped", 0, writer.getDocStats().numDocs);
    }

    /**
     * Refresh with no files skips addIndexes.
     */
    public void testRefreshWithNoLuceneFilesSkipsAddIndexes() throws IOException {
        LuceneIndexingExecutionEngine engine = new LuceneIndexingExecutionEngine(committer.getIndexWriter());

        RefreshInput emptyInput = RefreshInput.builder().build();
        RefreshResult result = engine.refresh(emptyInput);
        assertNotNull(result);
        assertEquals(0, committer.getIndexWriter().getDocStats().numDocs);
    }

    /**
     * Refresh with no parent writer is a no-op.
     */
    public void testRefreshWithoutParentWriterIsNoOp() throws IOException {
        LuceneIndexingExecutionEngine engine = new LuceneIndexingExecutionEngine(null);

        RefreshInput input = RefreshInput.builder().build();
        RefreshResult result = engine.refresh(input);
        assertNotNull(result);
        assertTrue(result.refreshedSegments().isEmpty());
    }

    /**
     * Refresh with null input returns empty result.
     */
    public void testRefreshWithNullInputReturnsEmpty() throws IOException {
        LuceneIndexingExecutionEngine engine = new LuceneIndexingExecutionEngine(committer.getIndexWriter());

        RefreshResult result = engine.refresh(null);
        assertNotNull(result);
        assertTrue(result.refreshedSegments().isEmpty());
    }
}
