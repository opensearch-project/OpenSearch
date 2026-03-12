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
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.core.index.shard.ShardId;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;

/**
 * A {@link org.apache.lucene.index.FilterDirectoryReader} that exposes
 * OpenSearch internal per shard / index information like the shard ID.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class OpenSearchDirectoryReader extends FilterDirectoryReader {

    public static final String CONTEXT_AWARE_BUCKET_ATTRIBUTE = "bucket";

    private final ShardId shardId;
    private final FilterDirectoryReader.SubReaderWrapper wrapper;

    private final DelegatingCacheHelper delegatingCacheHelper;

    /**
     * Map of criteria to their corresponding LeafReaders. Each reader is a segment
     * that match a specific criterion. This mapping avoids runtime penalties during search
     * by maintaining pre-filtered readers. Used for search on context-aware indices.
     */
    private Map<String, List<LeafReader>> contextAwareReadersLeafReaderMap;

    private OpenSearchDirectoryReader(DirectoryReader in, FilterDirectoryReader.SubReaderWrapper wrapper, ShardId shardId)
        throws IOException {
        super(in, wrapper);
        this.wrapper = wrapper;
        this.shardId = shardId;
        this.delegatingCacheHelper = new DelegatingCacheHelper(in.getReaderCacheHelper());

        // Do not do this warm up if its CriteriaBasedReader as criteriaBasedReaders are already formed
        // This should ideally not happen but just a safety check
        if (!(in instanceof ChildDirectoryReader)) {
            warmUpCriteriaBasedReader(in.leaves());
        }
    }

    private OpenSearchDirectoryReader(
        DirectoryReader in,
        FilterDirectoryReader.SubReaderWrapper wrapper,
        ShardId shardId,
        String uniqCacheKey
    ) throws IOException {

        super(in, wrapper);
        this.wrapper = wrapper;
        this.shardId = shardId;
        this.delegatingCacheHelper = new DelegatingCacheHelper(in.getReaderCacheHelper(), uniqCacheKey);
    }

    /**
     * Initializes the criteria-based reader mapping by scanning all leaf readers and building
     * a lookup table that maps criteria strings to their corresponding segments.
     *
     * <p>This method performs the essential preprocessing step for context-aware indices by:
     * <ul>
     *   <li>Examining each segment's metadata for "bucket" attributes</li>
     *   <li>Building a {@link HashMap} that groups segments by their criteria</li>
     *   <li>Enabling O(1) lookup performance for {@link #getCriteriaBasedReader(Set)}</li>
     * </ul>
     *
     * <p><strong>Performance Characteristics:</strong>
     * <ul>
     *   <li>Time Complexity: O(n) where n is the number of segments</li>
     *   <li>Called once during reader initialization to amortize costs</li>
     * </ul>
     *
     * <p><strong>Implementation Details:</strong>
     * The method iterates through all {@link LeafReaderContext} instances and:
     * <ol>
     *   <li>Extracts the segment reader using {@link Lucene#segmentReader(LeafReader)}</li>
     *   <li>Retrieves the "bucket" attribute from segment metadata</li>
     *   <li>Groups segments with the same criteria into lists</li>
     *   <li>Skips segments without criteria attributes (non-context-aware segments)</li>
     * </ol>
     *
     * <p><strong>Usage Context:</strong>
     * This method is automatically called during {@link OpenSearchDirectoryReader} construction
     * for regular directory readers (not {@link ChildDirectoryReader} instances). The warm-up
     * process ensures that subsequent calls to {@link #getCriteriaBasedReader(Set)} can
     * efficiently filter segments without scanning metadata repeatedly.
     *
     * <p><strong>Thread Safety:</strong>
     * This method is called during object construction and is not thread-safe. However,
     * once initialized, the resulting {@link #contextAwareReadersLeafReaderMap} can be
     * safely accessed concurrently for read operations.
     *
     * @param leaves The list of {@link LeafReaderContext} instances representing all segments
     *               in the directory reader. Cannot be null.
     *
     * @implNote Segments without "bucket" attributes are silently ignored, allowing mixed
     *           indices with both context-aware and regular segments.
     *
     * @since 3.4
     * @see #getCriteriaBasedReader(Set) for how the built mappings are used
     * @see #contextAwareReadersLeafReaderMap for the resulting data structure
     */
    private void warmUpCriteriaBasedReader(final List<LeafReaderContext> leaves) {
        this.contextAwareReadersLeafReaderMap = new HashMap<>();
        for (LeafReaderContext leafReaderContext : leaves) {
            final String criteria = Lucene.segmentReader(leafReaderContext.reader()).getSegmentInfo().info.getAttribute(
                CONTEXT_AWARE_BUCKET_ATTRIBUTE
            );
            if (criteria != null) {
                contextAwareReadersLeafReaderMap.computeIfAbsent(criteria, k -> new ArrayList<>()).add(leafReaderContext.reader());
            }
        }
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

    /**
     * Creates a criteria-based directory reader that contains only segments matching the specified criteria.
     * This method is designed for context-aware indices where segments are tagged with specific criteria
     * (stored as "bucket" attributes in segment metadata).
     *
     * <p>The method filters segments based on the provided criteria set and returns a new
     * {@link OpenSearchDirectoryReader} containing only the matching segments. This enables efficient
     * search operations on subsets of data without scanning irrelevant segments.
     *
     * <p><strong>Performance Benefits:</strong>
     * <ul>
     *   <li>Reduces search scope by excluding non-matching segments</li>
     *   <li>Leverages pre-computed segment-to-criteria mappings for fast filtering</li>
     *   <li>Maintains separate cache keys for different criteria combinations</li>
     * </ul>
     *
     * <p><strong>Usage Example:</strong>
     * <pre>{@code
     * Set<String> criteria = Set.of("tenant1", "region-us-west");
     * OpenSearchDirectoryReader filteredReader = reader.getCriteriaBasedReader(criteria);
     * // Use filteredReader for searches that should only include segments matching the criteria
     * }</pre>
     *
     * <p><strong>Implementation Details:</strong>
     * <ul>
     *   <li>Uses {@link #contextAwareReadersLeafReaderMap} for O(1) criteria lookup</li>
     *   <li>Creates a unique cache key based on the criteria combination</li>
     *   <li>Returns a {@link ChildDirectoryReader} wrapping the filtered segments</li>
     *   <li>Empty criteria sets return readers with no segments</li>
     * </ul>
     *
     * @param criteria A set of criteria strings to filter segments by. Each criterion should match
     *                 the "bucket" attribute stored in segment metadata. Cannot be null.
     * @return A new {@link OpenSearchDirectoryReader} containing only segments that match any of the
     *         specified criteria. If no segments match, returns a reader with no segments.
     * @throws IOException If there is an error creating the child directory reader or accessing segment metadata
     * @throws IllegalArgumentException If criteria is null
     *
     * @since 3.4
     * @see #warmUpCriteriaBasedReader(List) for how segment-to-criteria mappings are built
     * @see ChildDirectoryReader for the underlying reader implementation
     */
    public OpenSearchDirectoryReader getCriteriaBasedReader(Set<String> criteria) throws IOException {
        final List<LeafReader> leafReaders = new ArrayList<>();
        for (final String criteriaKey : criteria) {
            leafReaders.addAll(contextAwareReadersLeafReaderMap.getOrDefault(criteriaKey, emptyList()));
        }
        return createChildDirectoryReader(leafReaders, criteria);
    }

    private OpenSearchDirectoryReader createChildDirectoryReader(final List<LeafReader> leafReaders, Set<String> criteria)
        throws IOException {
        final String uniqCacheKey = UUID.nameUUIDFromBytes(
            criteria.stream().sorted().collect(Collectors.joining(",")).getBytes(StandardCharsets.UTF_8)
        ).toString();
        return new OpenSearchDirectoryReader(
            new ChildDirectoryReader(in.directory(), leafReaders.toArray(new LeafReader[0])),
            wrapper,
            this.shardId,
            uniqCacheKey
        );
    }

    public DelegatingCacheHelper getDelegatingCacheHelper() {
        return this.delegatingCacheHelper;
    }

    /**
     * Wraps existing IndexReader cache helper which internally provides a way to wrap CacheKey.
     *
     * @opensearch.api
     */
    @PublicApi(since = "2.13.0")
    public class DelegatingCacheHelper implements CacheHelper {
        private final CacheHelper cacheHelper;
        private final DelegatingCacheKey serializableCacheKey;

        DelegatingCacheHelper(CacheHelper cacheHelper) {
            this.cacheHelper = cacheHelper;
            this.serializableCacheKey = new DelegatingCacheKey(Optional.ofNullable(cacheHelper).map(key -> getKey()).orElse(null));
        }

        DelegatingCacheHelper(CacheHelper cacheHelper, String delegatingCacheKeyUniqId) {
            this.cacheHelper = cacheHelper;
            this.serializableCacheKey = new DelegatingCacheKey(
                Optional.ofNullable(cacheHelper).map(key -> getKey()).orElse(null),
                delegatingCacheKeyUniqId
            );
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
     * Wraps internal IndexReader.CacheKey and attaches a uniqueId to it which can be eventually be used instead of
     * object itself for serialization purposes.
     *
     * @opensearch.api
     */
    @PublicApi(since = "2.13.0")
    public class DelegatingCacheKey {
        private final CacheKey cacheKey;
        private final String uniqueId;

        DelegatingCacheKey(CacheKey cacheKey) {
            this.cacheKey = cacheKey;
            this.uniqueId = UUID.randomUUID().toString();
        }

        DelegatingCacheKey(CacheKey cacheKey, String uniqueId) {
            this.cacheKey = cacheKey;
            this.uniqueId = uniqueId;
        }

        public CacheKey getCacheKey() {
            return this.cacheKey;
        }

        public String getId() {
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
     * @param reader  the reader to wrap
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
     *                                  {@link OpenSearchDirectoryReader} in it's hierarchy
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

    /**
     * A specialized DirectoryReader implementation that holds a subset of {@link LeafReader}s
     * This reader provides functionality to selectively holds segments extracted from a parent directory for
     * context aware indices
     *
     * <p>ChildDirectoryReader wraps a set of LeafReaders in a directory. This is particularly useful when a directory
     * needs to wrapped around a set of segments
     *
     * <p>This implementation provides a specialized way to handle segment-level operations with the following characteristics:
     *  <ul>
     *    <li>Acts as a child reader to a parent directory reader</li>
     *    <li>Maintains its own cache helper with a unique cache key UUID</li>
     *    <li>Does not support index modifications or version operations</li>
     *    <li>Delegates current state checking to the parent OpenSearchDirectoryReader</li>
     *    <li>Lifecycle is controlled by the parent reader - all child readers are automatically closed when the parent closes</li>
     *  </ul>
     *
     * <p><strong>Important:</strong> This class is marked as experimental and its API might change in future releases.
     * <p><strong>Lifecycle Note:</strong> The lifecycle of this reader is managed by its parent reader. When the parent
     *  reader is closed, all associated child readers will be automatically closed as well. Therefore, there's no need
     *  to explicitly close child readers. Attempting to access a closed child reader might result in an exception.
     *
     * @since 3.4
     * @see org.apache.lucene.index.DirectoryReader
     * @see org.apache.lucene.index.LeafReader
     */
    @ExperimentalApi
    public class ChildDirectoryReader extends DirectoryReader {

        /**
         * Constructs a new ChildDirectoryReader instance.
         *
         * @param parentDirectory   The parent parentDirectory containing the index
         * @param childReaders      The array of LeafReaders to be wrapped. This array is returned by {@link
         *                          #getSequentialSubReaders} and used to resolve the correct subreader for docID-based
         *                          methods. <b>Please note:</b> This array is <b>not</b> cloned and not protected for
         *                          modification, the subclass is responsible to do this.
         * @throws IOException      If there is an error accessing the parentDirectory or readers
         */
        private ChildDirectoryReader(Directory parentDirectory, LeafReader[] childReaders) throws IOException {
            super(parentDirectory, childReaders, null);
        }

        /**
         * Closes the reader. This implementation is a no-op as the parent directory
         * is responsible for closing operations.
         *
         * @throws IOException If there is an error during close
         */
        @Override
        protected void doClose() throws IOException {
            // NO op - Parent directory should not be closed by child
        }

        /**
         * Returns the cache helper associated with this reader.
         *
         * @return The CacheHelper instance used by this reader
         */
        @Override
        public CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }

        /**
         * Checks if the reader is current by delegating to the parent OpenSearchDirectoryReader.
         *
         * @return true if the reader is current, false otherwise
         * @throws IOException If there is an error checking the current state
         */
        @Override
        public boolean isCurrent() throws IOException {
            // Should inherit parent directory value. If parent is not current, sub directory reader won't be current
            return OpenSearchDirectoryReader.this.isCurrent();
        }

        /**
         * Operations not supported by this implementation:
         * <ul>
         *   <li>{@link #doOpenIfChanged()} - Does not support reader refresh operations</li>
         *   <li>{@link #doOpenIfChanged(IndexCommit)} - Does not support commit-based reader refresh</li>
         *   <li>{@link #doOpenIfChanged(IndexWriter, boolean)} - Does not support writer-based reader refresh</li>
         *   <li>{@link #getVersion()} - Version operations are not supported</li>
         *   <li>{@link #getIndexCommit()} - Index commit operations are not supported</li>
         * </ul>
         *
         * @throws UnsupportedOperationException when any of these operations are called
         */
        @Override
        protected DirectoryReader doOpenIfChanged() throws IOException {
            throw new UnsupportedOperationException("Criteria based reader does not support doOpenIfChanged() operation");
        }

        @Override
        protected DirectoryReader doOpenIfChanged(IndexCommit commit) throws IOException {
            throw new UnsupportedOperationException("Criteria based reader does not support doOpenIfChanged(IndexCommit) operation");
        }

        @Override
        protected DirectoryReader doOpenIfChanged(IndexWriter writer, boolean applyAllDeletes) throws IOException {
            throw new UnsupportedOperationException(
                "Criteria based reader does not support doOpenIfChanged(IndexWriter writer, boolean applyAllDeletes) operation"
            );
        }

        @Override
        public long getVersion() {
            throw new UnsupportedOperationException("Criteria based reader does not support getVersion() operation");
        }

        @Override
        public IndexCommit getIndexCommit() throws IOException {
            throw new UnsupportedOperationException("Criteria based reader does not support getIndexCommit() operation");
        }
    }
}
