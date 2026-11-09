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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.search.internal;

import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TotalHits;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.lease.Releasable;
import org.opensearch.search.Scroll;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Wrapper around information that needs to stay around when scrolling.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class ScrollContext implements Releasable {
    public TotalHits totalHits = null;
    public float maxScore = Float.NaN;
    public ScoreDoc lastEmittedDoc;
    public Scroll scroll;

    /**
     * Cache for sequential stored field readers per segment.
     * These readers are optimized for sequential access and cache decompressed blocks.
     *
     * Thread-safety note: Scroll requests are serialized (client waits for response before
     * sending next request), so while different threads may use this cache, they won't
     * access it concurrently. The underlying StoredFieldsReader has mutable state (BlockState)
     * but is safe for sequential single-threaded access across different threads.
     */
    private Map<Object, StoredFieldsReader> sequentialReaderCache;

    public StoredFieldsReader getCachedSequentialReader(Object segmentKey) {
        return sequentialReaderCache != null ? sequentialReaderCache.get(segmentKey) : null;
    }

    public void cacheSequentialReader(Object segmentKey, StoredFieldsReader reader) {
        if (sequentialReaderCache == null) {
            sequentialReaderCache = new HashMap<>();
        }
        sequentialReaderCache.put(segmentKey, reader);
    }

    @Override
    public void close() {
        if (sequentialReaderCache != null) {
            for (StoredFieldsReader reader : sequentialReaderCache.values()) {
                try {
                    reader.close();
                } catch (IOException e) {}
            }
            sequentialReaderCache = null;
        }
    }
}
