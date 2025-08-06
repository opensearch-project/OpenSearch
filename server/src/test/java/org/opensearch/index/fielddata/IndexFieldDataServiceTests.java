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

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.util.Accountable;
import org.opensearch.common.SetOnce;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexService;
import org.opensearch.index.fielddata.plain.SortedNumericIndexFieldData;
import org.opensearch.index.fielddata.plain.SortedSetOrdinalsIndexFieldData;
import org.opensearch.index.mapper.BooleanFieldMapper;
import org.opensearch.index.mapper.ContentPath;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.Mapper.BuilderContext;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.mapper.TextFieldMapper;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.cluster.IndicesClusterStateService;
import org.opensearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.lookup.SearchLookup;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.mockito.Mockito;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IndexFieldDataServiceTests extends OpenSearchSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class);
    }

    public void testGetForFieldDefaults() {
        final IndexService indexService = createIndex("test");
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexFieldDataService ifdService = new IndexFieldDataService(
            indexService.getIndexSettings(),
            indicesService.getIndicesFieldDataCache(),
            indicesService.getCircuitBreakerService(),
            indexService.mapperService(),
            indexService.getThreadPool()
        );
        final BuilderContext ctx = new BuilderContext(indexService.getIndexSettings().getSettings(), new ContentPath(1));
        final MappedFieldType stringMapper = new KeywordFieldMapper.Builder("string").build(ctx).fieldType();
        ifdService.clear();
        IndexFieldData<?> fd = ifdService.getForField(stringMapper, "test", () -> { throw new UnsupportedOperationException(); });
        assertTrue(fd instanceof SortedSetOrdinalsIndexFieldData);

        for (MappedFieldType mapper : Arrays.asList(
            new NumberFieldMapper.Builder("int", NumberFieldMapper.NumberType.BYTE, false, true).build(ctx).fieldType(),
            new NumberFieldMapper.Builder("int", NumberFieldMapper.NumberType.SHORT, false, true).build(ctx).fieldType(),
            new NumberFieldMapper.Builder("int", NumberFieldMapper.NumberType.INTEGER, false, true).build(ctx).fieldType(),
            new NumberFieldMapper.Builder("long", NumberFieldMapper.NumberType.LONG, false, true).build(ctx).fieldType()
        )) {
            ifdService.clear();
            fd = ifdService.getForField(mapper, "test", () -> { throw new UnsupportedOperationException(); });
            assertTrue(fd instanceof SortedNumericIndexFieldData);
        }

        final MappedFieldType floatMapper = new NumberFieldMapper.Builder("float", NumberFieldMapper.NumberType.FLOAT, false, true).build(
            ctx
        ).fieldType();
        ifdService.clear();
        fd = ifdService.getForField(floatMapper, "test", () -> { throw new UnsupportedOperationException(); });
        assertTrue(fd instanceof SortedNumericIndexFieldData);

        final MappedFieldType doubleMapper = new NumberFieldMapper.Builder("double", NumberFieldMapper.NumberType.DOUBLE, false, true)
            .build(ctx)
            .fieldType();
        ifdService.clear();
        fd = ifdService.getForField(doubleMapper, "test", () -> { throw new UnsupportedOperationException(); });
        assertTrue(fd instanceof SortedNumericIndexFieldData);
    }

    public void testIndexFieldDataCacheIsCleredAfterIndexRemoval() throws IOException, InterruptedException {
        final IndexService indexService = createIndex("test");
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        // copy the ifdService since we can set the listener only once.
        final IndexFieldDataService ifdService = new IndexFieldDataService(
            indexService.getIndexSettings(),
            indicesService.getIndicesFieldDataCache(),
            indicesService.getCircuitBreakerService(),
            indexService.mapperService(),
            indexService.getThreadPool()
        );

        final BuilderContext ctx = new BuilderContext(indexService.getIndexSettings().getSettings(), new ContentPath(1));
        final MappedFieldType mapper1 = new TextFieldMapper.Builder("field_1", createDefaultIndexAnalyzers()).fielddata(true)
            .build(ctx)
            .fieldType();
        final MappedFieldType mapper2 = new TextFieldMapper.Builder("field_2", createDefaultIndexAnalyzers()).fielddata(true)
            .build(ctx)
            .fieldType();
        final IndexWriter writer = new IndexWriter(new ByteBuffersDirectory(), new IndexWriterConfig(new KeywordAnalyzer()));
        Document doc = new Document();
        doc.add(new StringField("field_1", "thisisastring", Store.NO));
        doc.add(new StringField("field_2", "thisisanotherstring", Store.NO));
        writer.addDocument(doc);
        final IndexReader reader = DirectoryReader.open(writer);
        final AtomicInteger onCacheCalled = new AtomicInteger();
        final AtomicInteger onRemovalCalled = new AtomicInteger();
        ifdService.setListener(new IndexFieldDataCache.Listener() {
            @Override
            public void onCache(ShardId shardId, String fieldName, Accountable ramUsage) {}

            @Override
            public void onRemoval(ShardId shardId, String fieldName, boolean wasEvicted, long sizeInBytes) {}
        });
        IndexFieldData<?> ifd1 = ifdService.getForField(mapper1, "test", () -> { throw new UnsupportedOperationException(); });
        IndexFieldData<?> ifd2 = ifdService.getForField(mapper2, "test", () -> { throw new UnsupportedOperationException(); });
        LeafReaderContext leafReaderContext = reader.getContext().leaves().get(0);
        LeafFieldData loadField1 = ifd1.load(leafReaderContext);
        LeafFieldData loadField2 = ifd2.load(leafReaderContext);

        assertEquals(2, indicesService.getIndicesFieldDataCache().getCache().count());

        // Remove index
        indicesService.removeIndex(
            indexService.index(),
            IndicesClusterStateService.AllocatedIndices.IndexRemovalReason.DELETED,
            "Please delete!"
        );

        waitUntil(() -> indicesService.getIndicesFieldDataCache().getCache().count() == 0);

        reader.close();
        loadField1.close();
        loadField2.close();
        writer.close();
    }

    public void testGetForFieldRuntimeField() {
        final IndexService indexService = createIndex("test");
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexFieldDataService ifdService = new IndexFieldDataService(
            indexService.getIndexSettings(),
            indicesService.getIndicesFieldDataCache(),
            indicesService.getCircuitBreakerService(),
            indexService.mapperService(),
            indexService.getThreadPool()
        );
        final SetOnce<Supplier<SearchLookup>> searchLookupSetOnce = new SetOnce<>();
        MappedFieldType ft = mock(MappedFieldType.class);
        final int shardId = randomInt();
        when(ft.fielddataBuilder(Mockito.any(), Mockito.any())).thenAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            Supplier<SearchLookup> searchLookup = (Supplier<SearchLookup>) invocationOnMock.getArguments()[1];
            searchLookupSetOnce.set(searchLookup);
            assertEquals(searchLookup.get().shardId(), shardId);
            return (IndexFieldData.Builder) (cache, breakerService) -> null;
        });
        SearchLookup searchLookup = new SearchLookup(null, null, shardId);
        ifdService.getForField(ft, "qualified", () -> searchLookup);
        assertSame(searchLookup, searchLookupSetOnce.get().get());
    }

    public void testClearField() throws Exception {
        final IndexService indexService = createIndex("test");
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        // copy the ifdService since we can set the listener only once.
        final IndexFieldDataService ifdService = new IndexFieldDataService(
            indexService.getIndexSettings(),
            indicesService.getIndicesFieldDataCache(),
            indicesService.getCircuitBreakerService(),
            indexService.mapperService(),
            indexService.getThreadPool()
        );

        final BuilderContext ctx = new BuilderContext(indexService.getIndexSettings().getSettings(), new ContentPath(1));
        final MappedFieldType mapper1 = new TextFieldMapper.Builder("field_1", createDefaultIndexAnalyzers()).fielddata(true)
            .build(ctx)
            .fieldType();
        final MappedFieldType mapper2 = new TextFieldMapper.Builder("field_2", createDefaultIndexAnalyzers()).fielddata(true)
            .build(ctx)
            .fieldType();
        final IndexWriter writer = new IndexWriter(new ByteBuffersDirectory(), new IndexWriterConfig(new KeywordAnalyzer()));
        Document doc = new Document();
        doc.add(new StringField("field_1", "thisisastring", Store.NO));
        doc.add(new StringField("field_2", "thisisanotherstring", Store.NO));
        writer.addDocument(doc);
        final IndexReader reader = DirectoryReader.open(writer);
        final AtomicInteger onCacheCalled = new AtomicInteger();
        final AtomicInteger onRemovalCalled = new AtomicInteger();
        ifdService.setListener(new IndexFieldDataCache.Listener() {
            @Override
            public void onCache(ShardId shardId, String fieldName, Accountable ramUsage) {
                onCacheCalled.incrementAndGet();
            }

            @Override
            public void onRemoval(ShardId shardId, String fieldName, boolean wasEvicted, long sizeInBytes) {
                onRemovalCalled.incrementAndGet();
            }
        });
        IndexFieldData<?> ifd1 = ifdService.getForField(mapper1, "test", () -> { throw new UnsupportedOperationException(); });
        IndexFieldData<?> ifd2 = ifdService.getForField(mapper2, "test", () -> { throw new UnsupportedOperationException(); });
        LeafReaderContext leafReaderContext = reader.getContext().leaves().get(0);
        LeafFieldData loadField1 = ifd1.load(leafReaderContext);
        LeafFieldData loadField2 = ifd2.load(leafReaderContext);

        assertEquals(2, onCacheCalled.get());
        assertEquals(0, onRemovalCalled.get());

        ifdService.clearField("field_1");

        assertEquals(2, onCacheCalled.get());
        assertEquals(1, onRemovalCalled.get());

        ifdService.clearField("field_1");

        assertEquals(2, onCacheCalled.get());
        assertEquals(1, onRemovalCalled.get());

        ifdService.clearField("field_2");

        assertEquals(2, onCacheCalled.get());
        assertEquals(2, onRemovalCalled.get());

        reader.close();
        loadField1.close();
        loadField2.close();
        writer.close();
        ifdService.clear();
    }

    public void testFieldDataCacheListener() throws Exception {
        final IndexService indexService = createIndex("test");
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        // copy the ifdService since we can set the listener only once.
        final IndexFieldDataService ifdService = new IndexFieldDataService(
            indexService.getIndexSettings(),
            indicesService.getIndicesFieldDataCache(),
            indicesService.getCircuitBreakerService(),
            indexService.mapperService(),
            indexService.getThreadPool()
        );

        final BuilderContext ctx = new BuilderContext(indexService.getIndexSettings().getSettings(), new ContentPath(1));
        final MappedFieldType mapper1 = new TextFieldMapper.Builder("s", createDefaultIndexAnalyzers()).fielddata(true)
            .build(ctx)
            .fieldType();
        final IndexWriter writer = new IndexWriter(new ByteBuffersDirectory(), new IndexWriterConfig(new KeywordAnalyzer()));
        Document doc = new Document();
        doc.add(new StringField("s", "thisisastring", Store.NO));
        writer.addDocument(doc);
        DirectoryReader open = DirectoryReader.open(writer);
        final boolean wrap = randomBoolean();
        final IndexReader reader = wrap ? OpenSearchDirectoryReader.wrap(open, new ShardId("test", "_na_", 1)) : open;
        final AtomicInteger onCacheCalled = new AtomicInteger();
        final AtomicInteger onRemovalCalled = new AtomicInteger();
        ifdService.setListener(new IndexFieldDataCache.Listener() {
            @Override
            public void onCache(ShardId shardId, String fieldName, Accountable ramUsage) {
                if (wrap) {
                    assertEquals(new ShardId("test", "_na_", 1), shardId);
                } else {
                    assertNull(shardId);
                }
                onCacheCalled.incrementAndGet();
            }

            @Override
            public void onRemoval(ShardId shardId, String fieldName, boolean wasEvicted, long sizeInBytes) {
                if (wrap) {
                    assertEquals(new ShardId("test", "_na_", 1), shardId);
                } else {
                    assertNull(shardId);
                }
                onRemovalCalled.incrementAndGet();
            }
        });
        IndexFieldData<?> ifd = ifdService.getForField(mapper1, "test", () -> { throw new UnsupportedOperationException(); });
        LeafReaderContext leafReaderContext = reader.getContext().leaves().get(0);
        LeafFieldData load = ifd.load(leafReaderContext);
        assertEquals(1, onCacheCalled.get());
        assertEquals(0, onRemovalCalled.get());
        reader.close();
        load.close();
        writer.close();
        assertEquals(1, onCacheCalled.get());
        assertEquals(1, onRemovalCalled.get());
        ifdService.clear();
    }

    public void testSetCacheListenerTwice() {
        final IndexService indexService = createIndex("test");
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexFieldDataService shardPrivateService = new IndexFieldDataService(
            indexService.getIndexSettings(),
            indicesService.getIndicesFieldDataCache(),
            indicesService.getCircuitBreakerService(),
            indexService.mapperService(),
            indexService.getThreadPool()
        );
        // set it the first time...
        shardPrivateService.setListener(new IndexFieldDataCache.Listener() {
            @Override
            public void onCache(ShardId shardId, String fieldName, Accountable ramUsage) {

            }

            @Override
            public void onRemoval(ShardId shardId, String fieldName, boolean wasEvicted, long sizeInBytes) {

            }
        });
        // now set it again and make sure we fail
        try {
            shardPrivateService.setListener(new IndexFieldDataCache.Listener() {
                @Override
                public void onCache(ShardId shardId, String fieldName, Accountable ramUsage) {

                }

                @Override
                public void onRemoval(ShardId shardId, String fieldName, boolean wasEvicted, long sizeInBytes) {

                }
            });
            fail("listener already set");
        } catch (IllegalStateException ex) {
            // all well
        }
    }

    private void doTestRequireDocValues(MappedFieldType ft) {
        ThreadPool threadPool = new TestThreadPool("random_threadpool_name");
        try {
            IndicesFieldDataCache cache = new IndicesFieldDataCache(Settings.EMPTY, null);
            IndexFieldDataService ifds = new IndexFieldDataService(
                IndexSettingsModule.newIndexSettings("test", Settings.EMPTY),
                cache,
                null,
                null,
                threadPool
            );
            if (ft.hasDocValues()) {
                ifds.getForField(ft, "test", () -> { throw new UnsupportedOperationException(); }); // no exception
            } else {
                IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ifds.getForField(ft, "test", () -> {
                    throw new UnsupportedOperationException();
                }));
                assertThat(e.getMessage(), containsString("doc values"));
            }
        } finally {
            threadPool.shutdown();
        }
    }

    public void testRequireDocValuesOnLongs() {
        doTestRequireDocValues(new NumberFieldMapper.NumberFieldType("field", NumberFieldMapper.NumberType.LONG));
        doTestRequireDocValues(
            new NumberFieldMapper.NumberFieldType(
                "field",
                NumberFieldMapper.NumberType.LONG,
                true,
                false,
                false,
                false,
                null,
                Collections.emptyMap()
            )
        );
    }

    public void testRequireDocValuesOnDoubles() {
        doTestRequireDocValues(new NumberFieldMapper.NumberFieldType("field", NumberFieldMapper.NumberType.DOUBLE));
        doTestRequireDocValues(
            new NumberFieldMapper.NumberFieldType(
                "field",
                NumberFieldMapper.NumberType.DOUBLE,
                true,
                false,
                false,
                false,
                null,
                Collections.emptyMap()
            )
        );
    }

    public void testRequireDocValuesOnUnsignedLongs() {
        doTestRequireDocValues(new NumberFieldMapper.NumberFieldType("field", NumberFieldMapper.NumberType.UNSIGNED_LONG));
        doTestRequireDocValues(
            new NumberFieldMapper.NumberFieldType(
                "field",
                NumberFieldMapper.NumberType.UNSIGNED_LONG,
                true,
                false,
                false,
                false,
                null,
                Collections.emptyMap()
            )
        );
    }

    public void testRequireDocValuesOnBools() {
        doTestRequireDocValues(new BooleanFieldMapper.BooleanFieldType("field"));
        doTestRequireDocValues(new BooleanFieldMapper.BooleanFieldType("field", true, false, false, null, Collections.emptyMap()));
    }
}
