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

package org.opensearch.common.lucene.index;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.core.index.shard.ShardId;

import java.io.IOException;
import java.util.UUID;

/**
 * A {@link org.apache.lucene.index.FilterDirectoryReader} that exposes
 * OpenSearch internal per shard / index information like the shard ID.
 *
 * @opensearch.internal
 */
public final class OpenSearchDirectoryReader extends FilterDirectoryReader {

    private final ShardId shardId;
    private final FilterDirectoryReader.SubReaderWrapper wrapper;

    private DelegatingCacheHelper delegatingCacheHelper;

    private OpenSearchDirectoryReader(DirectoryReader in, FilterDirectoryReader.SubReaderWrapper wrapper, ShardId shardId)
        throws IOException {
        super(in, wrapper);
        this.wrapper = wrapper;
        this.shardId = shardId;
        this.delegatingCacheHelper = new DelegatingCacheHelper(in.getReaderCacheHelper());
    }

    /**
     * Returns the shard id this index belongs to.
     */
    public ShardId shardId() {
        return this.shardId;
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        // safe to delegate since this reader does not alter the index
        return this.delegatingCacheHelper;
    }

    public DelegatingCacheHelper getDelegatingCacheHelper() {
        return this.delegatingCacheHelper;
    }

    /**
     * Wraps existing IndexReader cache helper which internally provides a way to wrap CacheKey.
     * @opensearch.internal
     */
    public class DelegatingCacheHelper implements CacheHelper {
        CacheHelper cacheHelper;
        DelegatingCacheKey serializableCacheKey;

        DelegatingCacheHelper(CacheHelper cacheHelper) {
            this.cacheHelper = cacheHelper;
            this.serializableCacheKey = new DelegatingCacheKey(cacheHelper.getKey());
        }

        @Override
        public CacheKey getKey() {
            return this.cacheHelper.getKey();
        }

        public DelegatingCacheKey getDelegatingCacheKey() {
            return this.serializableCacheKey;
        }

        @Override
        public void addClosedListener(ClosedListener listener) {
            this.cacheHelper.addClosedListener(listener);
        }
    }

    /**
     *  Wraps internal IndexReader.CacheKey and attaches a uniqueId to it which can be eventually be used instead of
     *  object itself for serialization purposes.
     */
    public class DelegatingCacheKey {
        CacheKey cacheKey;
        private final UUID uniqueId;

        DelegatingCacheKey(CacheKey cacheKey) {
            this.cacheKey = cacheKey;
            this.uniqueId = UUID.randomUUID();
        }

        public CacheKey getCacheKey() {
            return this.cacheKey;
        }

        public UUID getId() {
            return uniqueId;
        }
    }

    @Override
    protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
        return new OpenSearchDirectoryReader(in, wrapper, shardId);
    }

    /**
     * Wraps the given reader in a {@link OpenSearchDirectoryReader} as
     * well as all it's sub-readers in {@link OpenSearchLeafReader} to
     * expose the given shard Id.
     *
     * @param reader the reader to wrap
     * @param shardId the shard ID to expose via the opensearch internal reader wrappers.
     */
    public static OpenSearchDirectoryReader wrap(DirectoryReader reader, ShardId shardId) throws IOException {
        return new OpenSearchDirectoryReader(reader, new SubReaderWrapper(shardId), shardId);
    }

    private static final class SubReaderWrapper extends FilterDirectoryReader.SubReaderWrapper {
        private final ShardId shardId;

        SubReaderWrapper(ShardId shardId) {
            this.shardId = shardId;
        }

        @Override
        public LeafReader wrap(LeafReader reader) {
            return new OpenSearchLeafReader(reader, shardId);
        }
    }

    /**
     * Adds the given listener to the provided directory reader. The reader
     * must contain an {@link OpenSearchDirectoryReader} in it's hierarchy
     * otherwise we can't safely install the listener.
     *
     * @throws IllegalArgumentException if the reader doesn't contain an
     *     {@link OpenSearchDirectoryReader} in it's hierarchy
     */
    @SuppressForbidden(reason = "This is the only sane way to add a ReaderClosedListener")
    public static void addReaderCloseListener(DirectoryReader reader, IndexReader.ClosedListener listener) {
        OpenSearchDirectoryReader openSearchDirectoryReader = getOpenSearchDirectoryReader(reader);
        if (openSearchDirectoryReader == null) {
            throw new IllegalArgumentException(
                "Can't install close listener reader is not an OpenSearchDirectoryReader/OpenSearchLeafReader"
            );
        }
        IndexReader.CacheHelper cacheHelper = openSearchDirectoryReader.getReaderCacheHelper();
        if (cacheHelper == null) {
            throw new IllegalArgumentException("Reader " + openSearchDirectoryReader + " does not support caching");
        }
        assert cacheHelper.getKey() == reader.getReaderCacheHelper().getKey();
        cacheHelper.addClosedListener(listener);
    }

    /**
     * Tries to unwrap the given reader until the first
     * {@link OpenSearchDirectoryReader} instance is found or {@code null}
     * if no instance is found.
     */
    public static OpenSearchDirectoryReader getOpenSearchDirectoryReader(DirectoryReader reader) {
        if (reader instanceof FilterDirectoryReader) {
            if (reader instanceof OpenSearchDirectoryReader) {
                return (OpenSearchDirectoryReader) reader;
            } else {
                // We need to use FilterDirectoryReader#getDelegate and not FilterDirectoryReader#unwrap, because
                // If there are multiple levels of filtered leaf readers then with the unwrap() method it immediately
                // returns the most inner leaf reader and thus skipping of over any other filtered leaf reader that
                // may be instance of OpenSearchLeafReader. This can cause us to miss the shardId.
                return getOpenSearchDirectoryReader(((FilterDirectoryReader) reader).getDelegate());
            }
        }
        return null;
    }
}
