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

package org.opensearch.indices;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.CheckedSupplier;
import org.opensearch.common.cache.module.CacheModule;
import org.opensearch.common.cache.service.CacheService;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.common.bytes.AbstractBytesReference;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentHelper;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.IndexService;
import org.opensearch.index.cache.request.RequestCacheStats;
import org.opensearch.index.cache.request.ShardRequestCache;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardState;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IndicesRequestCacheTests extends OpenSearchSingleNodeTestCase {

    public void testBasicOperationsCache() throws Exception {
        IndexShard indexShard = createIndex("test").getShard(0);
        IndicesRequestCache cache = new IndicesRequestCache(
            Settings.EMPTY,
            (shardId -> Optional.of(new IndicesService.IndexShardCacheEntity(indexShard))),
            new CacheModule(new ArrayList<>(), Settings.EMPTY).getCacheService()
        );
        Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());

        writer.addDocument(newDoc(0, "foo"));
        DirectoryReader reader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));
        TermQueryBuilder termQuery = new TermQueryBuilder("id", "0");
        BytesReference termBytes = XContentHelper.toXContent(termQuery, MediaTypeRegistry.JSON, false);

        // initial cache
        IndicesService.IndexShardCacheEntity entity = new IndicesService.IndexShardCacheEntity(indexShard);
        Loader loader = new Loader(reader, 0);
        BytesReference value = cache.getOrCompute(entity, loader, reader, termBytes);
        assertEquals("foo", value.streamInput().readString());
        ShardRequestCache requestCacheStats = indexShard.requestCache();
        assertEquals(0, requestCacheStats.stats().getHitCount());
        assertEquals(1, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertFalse(loader.loadedFromCache);
        assertEquals(1, cache.count());

        // cache hit
        entity = new IndicesService.IndexShardCacheEntity(indexShard);
        loader = new Loader(reader, 0);
        value = cache.getOrCompute(entity, loader, reader, termBytes);
        assertEquals("foo", value.streamInput().readString());
        requestCacheStats = indexShard.requestCache();
        assertEquals(1, requestCacheStats.stats().getHitCount());
        assertEquals(1, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertTrue(loader.loadedFromCache);
        assertEquals(1, cache.count());
        assertTrue(requestCacheStats.stats().getMemorySize().bytesAsInt() > value.length());
        assertEquals(1, cache.numRegisteredCloseListeners());

        // Closing the cache doesn't modify an already returned CacheEntity
        if (randomBoolean()) {
            reader.close();
        } else {
            indexShard.close("test", true, true); // closed shard but reader is still open
            cache.clear(entity);
        }
        cache.cleanCache();
        assertEquals(1, requestCacheStats.stats().getHitCount());
        assertEquals(1, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertTrue(loader.loadedFromCache);
        assertEquals(0, cache.count());
        assertEquals(0, requestCacheStats.stats().getMemorySize().bytesAsInt());

        IOUtils.close(reader, writer, dir, cache);
        assertEquals(0, cache.numRegisteredCloseListeners());
    }

    public void testBasicOperationsCacheWithFeatureFlag() throws Exception {
        IndexShard indexShard = createIndex("test").getShard(0);
        CacheService cacheService = new CacheModule(new ArrayList<>(), Settings.EMPTY).getCacheService();
        IndicesRequestCache cache = new IndicesRequestCache(
            Settings.builder().put(super.featureFlagSettings()).put(FeatureFlags.PLUGGABLE_CACHE, "true").build(),
            (shardId -> Optional.of(new IndicesService.IndexShardCacheEntity(indexShard))),
            cacheService
        );
        Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());

        writer.addDocument(newDoc(0, "foo"));
        DirectoryReader reader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));
        TermQueryBuilder termQuery = new TermQueryBuilder("id", "0");
        BytesReference termBytes = XContentHelper.toXContent(termQuery, MediaTypeRegistry.JSON, false);

        // initial cache
        IndicesService.IndexShardCacheEntity entity = new IndicesService.IndexShardCacheEntity(indexShard);
        Loader loader = new Loader(reader, 0);
        BytesReference value = cache.getOrCompute(entity, loader, reader, termBytes);
        assertEquals("foo", value.streamInput().readString());
        ShardRequestCache requestCacheStats = indexShard.requestCache();
        assertEquals(0, requestCacheStats.stats().getHitCount());
        assertEquals(1, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertFalse(loader.loadedFromCache);
        assertEquals(1, cache.count());

        // cache hit
        entity = new IndicesService.IndexShardCacheEntity(indexShard);
        loader = new Loader(reader, 0);
        value = cache.getOrCompute(entity, loader, reader, termBytes);
        assertEquals("foo", value.streamInput().readString());
        requestCacheStats = indexShard.requestCache();
        assertEquals(1, requestCacheStats.stats().getHitCount());
        assertEquals(1, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertTrue(loader.loadedFromCache);
        assertEquals(1, cache.count());
        assertTrue(requestCacheStats.stats().getMemorySize().bytesAsInt() > value.length());
        assertEquals(1, cache.numRegisteredCloseListeners());

        // Closing the cache doesn't modify an already returned CacheEntity
        if (randomBoolean()) {
            reader.close();
        } else {
            indexShard.close("test", true, true); // closed shard but reader is still open
            cache.clear(entity);
        }
        cache.cleanCache();
        assertEquals(1, requestCacheStats.stats().getHitCount());
        assertEquals(1, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertTrue(loader.loadedFromCache);
        assertEquals(0, cache.count());
        assertEquals(0, requestCacheStats.stats().getMemorySize().bytesAsInt());

        IOUtils.close(reader, writer, dir, cache);
        assertEquals(0, cache.numRegisteredCloseListeners());
    }

    public void testCacheDifferentReaders() throws Exception {
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexShard indexShard = createIndex("test").getShard(0);
        IndicesRequestCache cache = new IndicesRequestCache(Settings.EMPTY, (shardId -> {
            IndexService indexService = null;
            try {
                indexService = indicesService.indexServiceSafe(shardId.getIndex());
            } catch (IndexNotFoundException ex) {
                return Optional.empty();
            }
            return Optional.of(new IndicesService.IndexShardCacheEntity(indexService.getShard(shardId.id())));
        }), new CacheModule(new ArrayList<>(), Settings.EMPTY).getCacheService());
        Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());

        writer.addDocument(newDoc(0, "foo"));
        DirectoryReader reader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));
        TermQueryBuilder termQuery = new TermQueryBuilder("id", "0");
        BytesReference termBytes = XContentHelper.toXContent(termQuery, MediaTypeRegistry.JSON, false);
        if (randomBoolean()) {
            writer.flush();
            IOUtils.close(writer);
            writer = new IndexWriter(dir, newIndexWriterConfig());
        }
        writer.updateDocument(new Term("id", "0"), newDoc(0, "bar"));
        DirectoryReader secondReader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));

        // initial cache
        IndicesService.IndexShardCacheEntity entity = new IndicesService.IndexShardCacheEntity(indexShard);
        Loader loader = new Loader(reader, 0);
        BytesReference value = cache.getOrCompute(entity, loader, reader, termBytes);
        ShardRequestCache requestCacheStats = entity.stats();
        assertEquals("foo", value.streamInput().readString());
        assertEquals(0, requestCacheStats.stats().getHitCount());
        assertEquals(1, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertFalse(loader.loadedFromCache);
        assertEquals(1, cache.count());
        assertTrue(requestCacheStats.stats().getMemorySize().bytesAsInt() > value.length());
        final int cacheSize = requestCacheStats.stats().getMemorySize().bytesAsInt();
        assertEquals(1, cache.numRegisteredCloseListeners());

        // cache the second
        IndicesService.IndexShardCacheEntity secondEntity = new IndicesService.IndexShardCacheEntity(indexShard);
        loader = new Loader(secondReader, 0);
        value = cache.getOrCompute(entity, loader, secondReader, termBytes);
        requestCacheStats = entity.stats();
        assertEquals("bar", value.streamInput().readString());
        assertEquals(0, requestCacheStats.stats().getHitCount());
        assertEquals(2, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertFalse(loader.loadedFromCache);
        assertEquals(2, cache.count());
        assertTrue(requestCacheStats.stats().getMemorySize().bytesAsInt() > cacheSize + value.length());
        assertEquals(2, cache.numRegisteredCloseListeners());

        secondEntity = new IndicesService.IndexShardCacheEntity(indexShard);
        loader = new Loader(secondReader, 0);
        value = cache.getOrCompute(secondEntity, loader, secondReader, termBytes);
        requestCacheStats = entity.stats();
        assertEquals("bar", value.streamInput().readString());
        assertEquals(1, requestCacheStats.stats().getHitCount());
        assertEquals(2, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertTrue(loader.loadedFromCache);
        assertEquals(2, cache.count());

        entity = new IndicesService.IndexShardCacheEntity(indexShard);
        loader = new Loader(reader, 0);
        value = cache.getOrCompute(entity, loader, reader, termBytes);
        assertEquals("foo", value.streamInput().readString());
        requestCacheStats = entity.stats();
        assertEquals(2, requestCacheStats.stats().getHitCount());
        assertEquals(2, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertTrue(loader.loadedFromCache);
        assertEquals(2, cache.count());

        // Closing the cache doesn't change returned entities
        reader.close();
        cache.cleanCache();
        assertEquals(2, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertTrue(loader.loadedFromCache);
        assertEquals(1, cache.count());
        assertEquals(cacheSize, requestCacheStats.stats().getMemorySize().bytesAsInt());
        assertEquals(1, cache.numRegisteredCloseListeners());

        // release
        if (randomBoolean()) {
            secondReader.close();
        } else {
            indexShard.close("test", true, true); // closed shard but reader is still open
            cache.clear(secondEntity);
        }
        cache.cleanCache();
        assertEquals(2, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertTrue(loader.loadedFromCache);
        assertEquals(0, cache.count());
        assertEquals(0, requestCacheStats.stats().getMemorySize().bytesAsInt());

        IOUtils.close(secondReader, writer, dir, cache);
        assertEquals(0, cache.numRegisteredCloseListeners());
    }

    public void testEviction() throws Exception {
        final ByteSizeValue size;
        {
            IndexShard indexShard = createIndex("test").getShard(0);
            IndicesRequestCache cache = new IndicesRequestCache(
                Settings.EMPTY,
                (shardId -> Optional.of(new IndicesService.IndexShardCacheEntity(indexShard))),
                new CacheModule(new ArrayList<>(), Settings.EMPTY).getCacheService()
            );
            Directory dir = newDirectory();
            IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());

            writer.addDocument(newDoc(0, "foo"));
            DirectoryReader reader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));
            TermQueryBuilder termQuery = new TermQueryBuilder("id", "0");
            BytesReference termBytes = XContentHelper.toXContent(termQuery, MediaTypeRegistry.JSON, false);
            IndicesService.IndexShardCacheEntity entity = new IndicesService.IndexShardCacheEntity(indexShard);
            Loader loader = new Loader(reader, 0);

            writer.updateDocument(new Term("id", "0"), newDoc(0, "bar"));
            DirectoryReader secondReader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));
            IndicesService.IndexShardCacheEntity secondEntity = new IndicesService.IndexShardCacheEntity(indexShard);
            Loader secondLoader = new Loader(secondReader, 0);

            BytesReference value1 = cache.getOrCompute(entity, loader, reader, termBytes);
            assertEquals("foo", value1.streamInput().readString());
            BytesReference value2 = cache.getOrCompute(secondEntity, secondLoader, secondReader, termBytes);
            assertEquals("bar", value2.streamInput().readString());
            size = indexShard.requestCache().stats().getMemorySize();
            IOUtils.close(reader, secondReader, writer, dir, cache);
        }
        IndexShard indexShard = createIndex("test1").getShard(0);
        IndicesRequestCache cache = new IndicesRequestCache(
            Settings.builder().put(IndicesRequestCache.INDICES_CACHE_QUERY_SIZE.getKey(), size.getBytes() + 1 + "b").build(),
            (shardId -> Optional.of(new IndicesService.IndexShardCacheEntity(indexShard))),
            new CacheModule(new ArrayList<>(), Settings.EMPTY).getCacheService()
        );
        Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());

        writer.addDocument(newDoc(0, "foo"));
        DirectoryReader reader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));
        TermQueryBuilder termQuery = new TermQueryBuilder("id", "0");
        BytesReference termBytes = XContentHelper.toXContent(termQuery, MediaTypeRegistry.JSON, false);
        IndicesService.IndexShardCacheEntity entity = new IndicesService.IndexShardCacheEntity(indexShard);
        Loader loader = new Loader(reader, 0);

        writer.updateDocument(new Term("id", "0"), newDoc(0, "bar"));
        DirectoryReader secondReader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));
        IndicesService.IndexShardCacheEntity secondEntity = new IndicesService.IndexShardCacheEntity(indexShard);
        Loader secondLoader = new Loader(secondReader, 0);

        writer.updateDocument(new Term("id", "0"), newDoc(0, "baz"));
        DirectoryReader thirdReader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));
        IndicesService.IndexShardCacheEntity thirddEntity = new IndicesService.IndexShardCacheEntity(indexShard);
        Loader thirdLoader = new Loader(thirdReader, 0);

        BytesReference value1 = cache.getOrCompute(entity, loader, reader, termBytes);
        assertEquals("foo", value1.streamInput().readString());
        BytesReference value2 = cache.getOrCompute(secondEntity, secondLoader, secondReader, termBytes);
        assertEquals("bar", value2.streamInput().readString());
        logger.info("Memory size: {}", indexShard.requestCache().stats().getMemorySize());
        BytesReference value3 = cache.getOrCompute(thirddEntity, thirdLoader, thirdReader, termBytes);
        assertEquals("baz", value3.streamInput().readString());
        assertEquals(2, cache.count());
        assertEquals(1, indexShard.requestCache().stats().getEvictions());
        IOUtils.close(reader, secondReader, thirdReader, writer, dir, cache);
    }

    public void testClearAllEntityIdentity() throws Exception {
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexShard indexShard = createIndex("test").getShard(0);
        IndicesRequestCache cache = new IndicesRequestCache(Settings.EMPTY, (shardId -> {
            IndexService indexService = null;
            try {
                indexService = indicesService.indexServiceSafe(shardId.getIndex());
            } catch (IndexNotFoundException ex) {
                return Optional.empty();
            }
            return Optional.of(new IndicesService.IndexShardCacheEntity(indexService.getShard(shardId.id())));
        }), new CacheModule(new ArrayList<>(), Settings.EMPTY).getCacheService());

        Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());

        writer.addDocument(newDoc(0, "foo"));
        DirectoryReader reader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));
        TermQueryBuilder termQuery = new TermQueryBuilder("id", "0");
        BytesReference termBytes = XContentHelper.toXContent(termQuery, MediaTypeRegistry.JSON, false);
        IndicesService.IndexShardCacheEntity entity = new IndicesService.IndexShardCacheEntity(indexShard);
        Loader loader = new Loader(reader, 0);

        writer.updateDocument(new Term("id", "0"), newDoc(0, "bar"));
        DirectoryReader secondReader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));
        IndicesService.IndexShardCacheEntity secondEntity = new IndicesService.IndexShardCacheEntity(indexShard);
        Loader secondLoader = new Loader(secondReader, 0);

        writer.updateDocument(new Term("id", "0"), newDoc(0, "baz"));
        DirectoryReader thirdReader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));
        IndicesService.IndexShardCacheEntity thirddEntity = new IndicesService.IndexShardCacheEntity(createIndex("test1").getShard(0));
        Loader thirdLoader = new Loader(thirdReader, 0);

        BytesReference value1 = cache.getOrCompute(entity, loader, reader, termBytes);
        assertEquals("foo", value1.streamInput().readString());
        BytesReference value2 = cache.getOrCompute(secondEntity, secondLoader, secondReader, termBytes);
        assertEquals("bar", value2.streamInput().readString());
        logger.info("Memory size: {}", indexShard.requestCache().stats().getMemorySize());
        BytesReference value3 = cache.getOrCompute(thirddEntity, thirdLoader, thirdReader, termBytes);
        assertEquals("baz", value3.streamInput().readString());
        assertEquals(3, cache.count());
        RequestCacheStats requestCacheStats = entity.stats().stats();
        requestCacheStats.add(thirddEntity.stats().stats());
        final long hitCount = requestCacheStats.getHitCount();
        // clear all for the indexShard Idendity even though is't still open
        cache.clear(randomFrom(entity, secondEntity));
        cache.cleanCache();
        assertEquals(1, cache.count());
        // third has not been validated since it's a different identity
        value3 = cache.getOrCompute(thirddEntity, thirdLoader, thirdReader, termBytes);
        requestCacheStats = entity.stats().stats();
        requestCacheStats.add(thirddEntity.stats().stats());
        assertEquals(hitCount + 1, requestCacheStats.getHitCount());
        assertEquals("baz", value3.streamInput().readString());

        IOUtils.close(reader, secondReader, thirdReader, writer, dir, cache);

    }

    public Iterable<Field> newDoc(int id, String value) {
        return Arrays.asList(
            newField("id", Integer.toString(id), StringField.TYPE_STORED),
            newField("value", value, StringField.TYPE_STORED)
        );
    }

    private static class Loader implements CheckedSupplier<BytesReference, IOException> {

        private final DirectoryReader reader;
        private final int id;
        public boolean loadedFromCache = true;

        Loader(DirectoryReader reader, int id) {
            super();
            this.reader = reader;
            this.id = id;
        }

        @Override
        public BytesReference get() {
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                TopDocs topDocs = searcher.search(new TermQuery(new Term("id", Integer.toString(id))), 1);
                assertEquals(1, topDocs.totalHits.value);
                Document document = reader.storedFields().document(topDocs.scoreDocs[0].doc);
                out.writeString(document.get("value"));
                loadedFromCache = false;
                return out.bytes();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

    }

    public void testInvalidate() throws Exception {
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexShard indexShard = createIndex("test").getShard(0);
        IndicesRequestCache cache = new IndicesRequestCache(Settings.EMPTY, (shardId -> {
            IndexService indexService = null;
            try {
                indexService = indicesService.indexServiceSafe(shardId.getIndex());
            } catch (IndexNotFoundException ex) {
                return Optional.empty();
            }
            return Optional.of(new IndicesService.IndexShardCacheEntity(indexService.getShard(shardId.id())));
        }), new CacheModule(new ArrayList<>(), Settings.EMPTY).getCacheService());
        Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());

        writer.addDocument(newDoc(0, "foo"));
        DirectoryReader reader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));
        TermQueryBuilder termQuery = new TermQueryBuilder("id", "0");
        BytesReference termBytes = XContentHelper.toXContent(termQuery, MediaTypeRegistry.JSON, false);

        // initial cache
        IndicesService.IndexShardCacheEntity entity = new IndicesService.IndexShardCacheEntity(indexShard);
        Loader loader = new Loader(reader, 0);
        BytesReference value = cache.getOrCompute(entity, loader, reader, termBytes);
        assertEquals("foo", value.streamInput().readString());
        ShardRequestCache requestCacheStats = entity.stats();
        assertEquals(0, requestCacheStats.stats().getHitCount());
        assertEquals(1, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertFalse(loader.loadedFromCache);
        assertEquals(1, cache.count());

        // cache hit
        entity = new IndicesService.IndexShardCacheEntity(indexShard);
        loader = new Loader(reader, 0);
        value = cache.getOrCompute(entity, loader, reader, termBytes);
        assertEquals("foo", value.streamInput().readString());
        requestCacheStats = entity.stats();
        assertEquals(1, requestCacheStats.stats().getHitCount());
        assertEquals(1, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertTrue(loader.loadedFromCache);
        assertEquals(1, cache.count());
        assertTrue(requestCacheStats.stats().getMemorySize().bytesAsInt() > value.length());
        assertEquals(1, cache.numRegisteredCloseListeners());

        // load again after invalidate
        entity = new IndicesService.IndexShardCacheEntity(indexShard);
        loader = new Loader(reader, 0);
        cache.invalidate(entity, reader, termBytes);
        value = cache.getOrCompute(entity, loader, reader, termBytes);
        assertEquals("foo", value.streamInput().readString());
        requestCacheStats = entity.stats();
        assertEquals(1, requestCacheStats.stats().getHitCount());
        assertEquals(2, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertFalse(loader.loadedFromCache);
        assertEquals(1, cache.count());
        assertTrue(requestCacheStats.stats().getMemorySize().bytesAsInt() > value.length());
        assertEquals(1, cache.numRegisteredCloseListeners());

        // release
        if (randomBoolean()) {
            reader.close();
        } else {
            indexShard.close("test", true, true); // closed shard but reader is still open
            cache.clear(entity);
        }
        cache.cleanCache();
        assertEquals(1, requestCacheStats.stats().getHitCount());
        assertEquals(2, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertEquals(0, cache.count());
        assertEquals(0, requestCacheStats.stats().getMemorySize().bytesAsInt());

        IOUtils.close(reader, writer, dir, cache);
        assertEquals(0, cache.numRegisteredCloseListeners());
    }

    public void testEqualsKey() throws IOException {
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        Directory dir = newDirectory();
        IndexWriterConfig config = newIndexWriterConfig();
        IndexWriter writer = new IndexWriter(dir, config);
        ShardId shardId = new ShardId("foo", "bar", 1);
        ShardId shardId1 = new ShardId("foo1", "bar1", 2);
        IndexReader reader1 = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), shardId);
        String rKey1 = ((OpenSearchDirectoryReader) reader1).getDelegatingCacheHelper().getDelegatingCacheKey().getId();
        writer.addDocument(new Document());
        IndexReader reader2 = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), shardId);
        String rKey2 = ((OpenSearchDirectoryReader) reader2).getDelegatingCacheHelper().getDelegatingCacheKey().getId();
        IOUtils.close(reader1, reader2, writer, dir);
        IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.state()).thenReturn(IndexShardState.STARTED);
        IndicesRequestCache.Key key1 = new IndicesRequestCache.Key(shardId, new TestBytesReference(1), rKey1);
        IndicesRequestCache.Key key2 = new IndicesRequestCache.Key(shardId, new TestBytesReference(1), rKey1);
        IndicesRequestCache.Key key3 = new IndicesRequestCache.Key(shardId1, new TestBytesReference(1), rKey1);
        IndicesRequestCache.Key key4 = new IndicesRequestCache.Key(shardId, new TestBytesReference(1), rKey2);
        IndicesRequestCache.Key key5 = new IndicesRequestCache.Key(shardId, new TestBytesReference(2), rKey2);
        String s = "Some other random object";
        assertEquals(key1, key1);
        assertEquals(key1, key2);
        assertNotEquals(key1, null);
        assertNotEquals(key1, s);
        assertNotEquals(key1, key3);
        assertNotEquals(key1, key4);
        assertNotEquals(key1, key5);
    }

    public void testSerializationDeserializationOfCacheKey() throws Exception {
        TermQueryBuilder termQuery = new TermQueryBuilder("id", "0");
        BytesReference termBytes = XContentHelper.toXContent(termQuery, MediaTypeRegistry.JSON, false);
        IndexService indexService = createIndex("test");
        IndexShard indexShard = indexService.getShard(0);
        IndicesService.IndexShardCacheEntity shardCacheEntity = new IndicesService.IndexShardCacheEntity(indexShard);
        String readerCacheKeyId = UUID.randomUUID().toString();
        IndicesRequestCache.Key key1 = new IndicesRequestCache.Key(indexShard.shardId(), termBytes, readerCacheKeyId);
        BytesReference bytesReference = null;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            key1.writeTo(out);
            bytesReference = out.bytes();
        }
        StreamInput in = bytesReference.streamInput();

        IndicesRequestCache.Key key2 = new IndicesRequestCache.Key(in);

        assertEquals(readerCacheKeyId, key2.readerCacheKeyId);
        assertEquals(((IndexShard) shardCacheEntity.getCacheIdentity()).shardId(), key2.shardId);
        assertEquals(termBytes, key2.value);

    }

    private class TestBytesReference extends AbstractBytesReference {

        int dummyValue;

        TestBytesReference(int dummyValue) {
            this.dummyValue = dummyValue;
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof TestBytesReference && this.dummyValue == ((TestBytesReference) other).dummyValue;
        }

        @Override
        public int hashCode() {
            int result = super.hashCode();
            result = 31 * result + dummyValue;
            return result;
        }

        @Override
        public byte get(int index) {
            return 0;
        }

        @Override
        public int length() {
            return 0;
        }

        @Override
        public BytesReference slice(int from, int length) {
            return null;
        }

        @Override
        public BytesRef toBytesRef() {
            return null;
        }

        @Override
        public long ramBytesUsed() {
            return 0;
        }

        @Override
        public boolean isFragment() {
            return false;
        }
    }
}
