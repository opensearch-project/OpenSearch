/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.prefetch;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.store.Directory;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.cache.bitset.BitsetFilterCache;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;

import static org.opensearch.common.settings.ClusterSettings.BUILT_IN_CLUSTER_SETTINGS;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StoredFieldsPrefetchTests extends OpenSearchTestCase {

    private ClusterService clusterService;
    private ThreadPool threadPool;
    private SearchContext searchContext;
    private TieredStoragePrefetchSettings tieredStoragePrefetchSettings;
    private StoredFieldsPrefetch storedFieldsPrefetch;
    private Directory directory;
    private IndexReader indexReader;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        Set<Setting<?>> clusterSettingsToAdd = new HashSet<>(BUILT_IN_CLUSTER_SETTINGS);
        clusterSettingsToAdd.add(TieredStoragePrefetchSettings.READ_AHEAD_BLOCK_COUNT);
        clusterSettingsToAdd.add(TieredStoragePrefetchSettings.STORED_FIELDS_PREFETCH_ENABLED_SETTING);
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, clusterSettingsToAdd);
        threadPool = new TestThreadPool("TieredStoragePrefetchSettingsTests");
        clusterService = ClusterServiceUtils.createClusterService(Settings.EMPTY, clusterSettings, threadPool);
        this.tieredStoragePrefetchSettings = new TieredStoragePrefetchSettings(clusterService.getClusterSettings());
        searchContext = mock(SearchContext.class);
        storedFieldsPrefetch = new StoredFieldsPrefetch(getPrefetchSettingsSupplier());

        directory = newDirectory();
        IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig());
        Document doc = new Document();
        doc.add(new StringField("id", "1", Field.Store.YES));
        writer.addDocument(doc);
        doc = new Document();
        doc.add(new StringField("id", "2", Field.Store.YES));
        writer.addDocument(doc);
        doc = new Document();
        doc.add(new StringField("id", "3", Field.Store.YES));
        writer.addDocument(doc);
        writer.commit();
        writer.close();
        indexReader = DirectoryReader.open(directory);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        indexReader.close();
        directory.close();
        threadPool.shutdownNow();
    }

    public Supplier<TieredStoragePrefetchSettings> getPrefetchSettingsSupplier() {
        return () -> this.tieredStoragePrefetchSettings;
    }

    /**
     * Helper to set up SearchContext mock with the real index reader and given doc IDs.
     */
    private void setupSearchContext(int[] docIds, boolean hasNested) {
        when(searchContext.docIdsToLoadSize()).thenReturn(docIds.length);
        when(searchContext.docIdsToLoad()).thenReturn(docIds);
        when(searchContext.docIdsToLoadFrom()).thenReturn(0);

        ContextIndexSearcher searcher = mock(ContextIndexSearcher.class);
        when(searchContext.searcher()).thenReturn(searcher);
        when(searcher.getIndexReader()).thenReturn(indexReader);

        IndexShard indexShard = mock(IndexShard.class);
        when(searchContext.indexShard()).thenReturn(indexShard);
        when(indexShard.shardId()).thenReturn(new ShardId("test-index", "uuid", 0));

        MapperService mapperService = mock(MapperService.class);
        when(searchContext.mapperService()).thenReturn(mapperService);
        when(mapperService.hasNested()).thenReturn(hasNested);

        if (hasNested) {
            IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(
                "test-index",
                Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build()
            );
            BitsetFilterCache bitsetFilterCache = new BitsetFilterCache(indexSettings, mock(BitsetFilterCache.Listener.class));
            when(searchContext.bitsetFilterCache()).thenReturn(bitsetFilterCache);
        }
    }

    public void testOnPreFetchPhase_WhenPrefetchDisabled() {
        Settings settings = Settings.builder()
            .put(TieredStoragePrefetchSettings.STORED_FIELDS_PREFETCH_ENABLED_SETTING.getKey(), false)
            .build();
        clusterService.getClusterSettings().applySettings(settings);
        storedFieldsPrefetch.onPreFetchPhase(searchContext);
        verify(searchContext, never()).docIdsToLoadSize();
    }

    public void testOnPreFetchPhase_WhenSettingsSupplierReturnsNull() {
        StoredFieldsPrefetch prefetchWithNull = new StoredFieldsPrefetch(() -> null);
        prefetchWithNull.onPreFetchPhase(searchContext);
        verify(searchContext, never()).docIdsToLoadSize();
    }

    public void testOnPreFetchPhase_WhenPrefetchEnabled_WithSegmentReader() throws IOException {
        setupSearchContext(new int[] { 0 }, false);
        storedFieldsPrefetch.onPreFetchPhase(searchContext);
        verify(searchContext, atLeastOnce()).docIdsToLoadSize();
    }

    public void testOnPreFetchPhase_WithNonSegmentReader_SkipsPrefetch() throws IOException {
        setupSearchContext(new int[] { 0 }, false);
        storedFieldsPrefetch.onPreFetchPhase(searchContext);
        verify(searchContext, atLeastOnce()).docIdsToLoadSize();
    }

    public void testOnPreFetchPhase_MultipleDocsInSameSegment() throws IOException {
        setupSearchContext(new int[] { 0, 1, 2 }, false);
        storedFieldsPrefetch.onPreFetchPhase(searchContext);
        verify(searchContext, atLeastOnce()).docIdsToLoadSize();
    }

    public void testOnPreFetchPhase_WithNestedMapping_NonNestedDoc() throws IOException {
        setupSearchContext(new int[] { 0 }, true);
        expectThrows(Exception.class, () -> storedFieldsPrefetch.onPreFetchPhase(searchContext));
    }

    public void testOnPreFetchPhase_WithNoNestedMapping() throws IOException {
        setupSearchContext(new int[] { 0 }, false);
        storedFieldsPrefetch.onPreFetchPhase(searchContext);
        verify(searchContext, atLeastOnce()).docIdsToLoadSize();
    }

    public void testOnPreFetchPhase_ExceptionWrappedAsOpenSearchException() throws IOException {
        when(searchContext.docIdsToLoadSize()).thenReturn(1);
        when(searchContext.docIdsToLoad()).thenReturn(new int[] { 0 });
        when(searchContext.docIdsToLoadFrom()).thenReturn(0);

        ContextIndexSearcher searcher = mock(ContextIndexSearcher.class);
        when(searchContext.searcher()).thenReturn(searcher);
        when(searcher.getIndexReader()).thenReturn(indexReader);

        IndexShard indexShard = mock(IndexShard.class);
        when(searchContext.indexShard()).thenReturn(indexShard);
        when(indexShard.shardId()).thenReturn(new ShardId("test-index", "uuid", 0));

        MapperService mapperService = mock(MapperService.class);
        when(searchContext.mapperService()).thenReturn(mapperService);
        when(mapperService.hasNested()).thenThrow(new RuntimeException("simulated failure"));

        expectThrows(Exception.class, () -> storedFieldsPrefetch.onPreFetchPhase(searchContext));
    }

    public void testOnPreFetchPhase_NullBitSetFilterCache_ThrowsException() throws IOException {
        when(searchContext.docIdsToLoadSize()).thenReturn(1);
        when(searchContext.docIdsToLoad()).thenReturn(new int[] { 0 });
        when(searchContext.docIdsToLoadFrom()).thenReturn(0);

        ContextIndexSearcher searcher = mock(ContextIndexSearcher.class);
        when(searchContext.searcher()).thenReturn(searcher);
        when(searcher.getIndexReader()).thenReturn(indexReader);

        IndexShard indexShard = mock(IndexShard.class);
        when(searchContext.indexShard()).thenReturn(indexShard);
        when(indexShard.shardId()).thenReturn(new ShardId("test-index", "uuid", 0));

        MapperService mapperService = mock(MapperService.class);
        when(searchContext.mapperService()).thenReturn(mapperService);
        when(mapperService.hasNested()).thenReturn(true);

        expectThrows(Exception.class, () -> storedFieldsPrefetch.onPreFetchPhase(searchContext));
    }

    public void testOnPreFetchPhase_WithNonSegmentReaderViaFilterDirectoryReader() throws IOException {
        DirectoryReader realReader = DirectoryReader.open(directory);
        DirectoryReader wrappedReader = new NonSegmentReaderDirectoryReader(realReader);
        try {
            when(searchContext.docIdsToLoadSize()).thenReturn(1);
            when(searchContext.docIdsToLoad()).thenReturn(new int[] { 0 });
            when(searchContext.docIdsToLoadFrom()).thenReturn(0);

            ContextIndexSearcher searcher = mock(ContextIndexSearcher.class);
            when(searchContext.searcher()).thenReturn(searcher);
            when(searcher.getIndexReader()).thenReturn(wrappedReader);

            IndexShard indexShard = mock(IndexShard.class);
            when(searchContext.indexShard()).thenReturn(indexShard);
            when(indexShard.shardId()).thenReturn(new ShardId("test-index", "uuid", 0));

            MapperService mapperService = mock(MapperService.class);
            when(searchContext.mapperService()).thenReturn(mapperService);
            when(mapperService.hasNested()).thenReturn(false);

            storedFieldsPrefetch.onPreFetchPhase(searchContext);

            verify(searchContext, atLeastOnce()).docIdsToLoadSize();
        } finally {
            wrappedReader.close();
        }
    }

    public void testOnPreFetchPhase_NullCurrentReaderContinuesForSubsequentDocs() throws IOException {
        DirectoryReader realReader = DirectoryReader.open(directory);
        DirectoryReader wrappedReader = new NonSegmentReaderDirectoryReader(realReader);
        try {
            when(searchContext.docIdsToLoadSize()).thenReturn(2);
            when(searchContext.docIdsToLoad()).thenReturn(new int[] { 0, 1 });
            when(searchContext.docIdsToLoadFrom()).thenReturn(0);

            ContextIndexSearcher searcher = mock(ContextIndexSearcher.class);
            when(searchContext.searcher()).thenReturn(searcher);
            when(searcher.getIndexReader()).thenReturn(wrappedReader);

            IndexShard indexShard = mock(IndexShard.class);
            when(searchContext.indexShard()).thenReturn(indexShard);
            when(indexShard.shardId()).thenReturn(new ShardId("test-index", "uuid", 0));

            MapperService mapperService = mock(MapperService.class);
            when(searchContext.mapperService()).thenReturn(mapperService);
            when(mapperService.hasNested()).thenReturn(false);

            storedFieldsPrefetch.onPreFetchPhase(searchContext);

            verify(searchContext, atLeastOnce()).docIdsToLoadSize();
        } finally {
            wrappedReader.close();
        }
    }

    public void testOnPreFetchPhase_NestedChildDoc_PrefetchesRootDoc() throws IOException {
        Directory nestedDir = newDirectory();
        IndexWriter writer = new IndexWriter(nestedDir, new IndexWriterConfig());
        Document childDoc = new Document();
        childDoc.add(new StringField("nested_field", "child_value", Field.Store.YES));
        Document rootDoc = new Document();
        rootDoc.add(new StringField("id", "1", Field.Store.YES));
        rootDoc.add(new NumericDocValuesField(SeqNoFieldMapper.PRIMARY_TERM_NAME, 1));
        writer.addDocuments(java.util.List.of(childDoc, rootDoc));
        writer.commit();
        writer.close();

        IndexReader nestedReader = DirectoryReader.open(nestedDir);
        try {
            when(searchContext.docIdsToLoadSize()).thenReturn(1);
            when(searchContext.docIdsToLoad()).thenReturn(new int[] { 0 });
            when(searchContext.docIdsToLoadFrom()).thenReturn(0);

            ContextIndexSearcher searcher = mock(ContextIndexSearcher.class);
            when(searchContext.searcher()).thenReturn(searcher);
            when(searcher.getIndexReader()).thenReturn(nestedReader);

            IndexShard indexShard = mock(IndexShard.class);
            when(searchContext.indexShard()).thenReturn(indexShard);
            when(indexShard.shardId()).thenReturn(new ShardId("test-index", "uuid", 0));

            MapperService mapperService = mock(MapperService.class);
            when(searchContext.mapperService()).thenReturn(mapperService);
            when(mapperService.hasNested()).thenReturn(true);

            IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(
                "test-index",
                Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build()
            );
            BitsetFilterCache bitsetFilterCache = new BitsetFilterCache(indexSettings, mock(BitsetFilterCache.Listener.class));
            when(searchContext.bitsetFilterCache()).thenReturn(bitsetFilterCache);

            expectThrows(Exception.class, () -> storedFieldsPrefetch.onPreFetchPhase(searchContext));
        } finally {
            nestedReader.close();
            nestedDir.close();
        }
    }

    private static class NonSegmentReaderDirectoryReader extends FilterDirectoryReader {
        NonSegmentReaderDirectoryReader(DirectoryReader in) throws IOException {
            super(in, new SubReaderWrapper() {
                @Override
                public LeafReader wrap(LeafReader reader) {
                    return new FilterLeafReader(reader) {
                        private final LeafReader fakeDelegate = mock(LeafReader.class);

                        @Override
                        public LeafReader getDelegate() {
                            return fakeDelegate;
                        }

                        @Override
                        public CacheHelper getCoreCacheHelper() {
                            return reader.getCoreCacheHelper();
                        }

                        @Override
                        public CacheHelper getReaderCacheHelper() {
                            return reader.getReaderCacheHelper();
                        }
                    };
                }
            });
        }

        @Override
        protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
            return new NonSegmentReaderDirectoryReader(in);
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }
    }
}
