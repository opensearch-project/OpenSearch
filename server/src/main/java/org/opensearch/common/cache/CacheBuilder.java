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

package org.opensearch.common.cache;

import org.opensearch.common.unit.TimeValue;

import java.util.Locale;
import java.util.Objects;
import java.util.function.ToLongBiFunction;

import static org.opensearch.common.cache.settings.CacheSettings.INVALID_SEGMENT_COUNT_EXCEPTION_MESSAGE;
import static org.opensearch.common.cache.settings.CacheSettings.VALID_SEGMENT_COUNT_VALUES;

/**
 * The cache builder.
 *
 * @opensearch.internal
 */
public class CacheBuilder<K, V> {
    private long maximumWeight = -1;
    private long expireAfterAccessNanos = -1;
    private long expireAfterWriteNanos = -1;
    private ToLongBiFunction<K, V> weigher;
    private RemovalListener<K, V> removalListener;
    private int numberOfSegments = -1;

    public static <K, V> CacheBuilder<K, V> builder() {
        return new CacheBuilder<>();
    }

    private CacheBuilder() {}

    public CacheBuilder<K, V> setNumberOfSegments(int numberOfSegments) {
        if (!VALID_SEGMENT_COUNT_VALUES.contains(numberOfSegments)) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, INVALID_SEGMENT_COUNT_EXCEPTION_MESSAGE, "Cache"));
        }
        this.numberOfSegments = numberOfSegments;
        return this;
    }

    public CacheBuilder<K, V> setMaximumWeight(long maximumWeight) {
        if (maximumWeight < 0) {
            throw new IllegalArgumentException("maximumWeight < 0");
        }
        this.maximumWeight = maximumWeight;
        return this;
    }

    /**
     * Sets the amount of time before an entry in the cache expires after it was last accessed.
     *
     * @param expireAfterAccess The amount of time before an entry expires after it was last accessed. Must not be {@code null} and must
     *                          be greater than 0.
     */
    public CacheBuilder<K, V> setExpireAfterAccess(TimeValue expireAfterAccess) {
        Objects.requireNonNull(expireAfterAccess);
        final long expireAfterAccessNanos = expireAfterAccess.getNanos();
        if (expireAfterAccessNanos <= 0) {
            throw new IllegalArgumentException("expireAfterAccess <= 0");
        }
        this.expireAfterAccessNanos = expireAfterAccessNanos;
        return this;
    }

    /**
     * Sets the amount of time before an entry in the cache expires after it was written.
     *
     * @param expireAfterWrite The amount of time before an entry expires after it was written. Must not be {@code null} and must be
     *                         greater than 0.
     */
    public CacheBuilder<K, V> setExpireAfterWrite(TimeValue expireAfterWrite) {
        Objects.requireNonNull(expireAfterWrite);
        final long expireAfterWriteNanos = expireAfterWrite.getNanos();
        if (expireAfterWriteNanos <= 0) {
            throw new IllegalArgumentException("expireAfterWrite <= 0");
        }
        this.expireAfterWriteNanos = expireAfterWriteNanos;
        return this;
    }

    public CacheBuilder<K, V> weigher(ToLongBiFunction<K, V> weigher) {
        Objects.requireNonNull(weigher);
        this.weigher = weigher;
        return this;
    }

    public CacheBuilder<K, V> removalListener(RemovalListener<K, V> removalListener) {
        Objects.requireNonNull(removalListener);
        this.removalListener = removalListener;
        return this;
    }

    public Cache<K, V> build() {
        Cache<K, V> cache = new Cache<>(numberOfSegments);
        if (maximumWeight != -1) {
            cache.setMaximumWeight(maximumWeight);
        }
        if (expireAfterAccessNanos != -1) {
            cache.setExpireAfterAccessNanos(expireAfterAccessNanos);
        }
        if (expireAfterWriteNanos != -1) {
            cache.setExpireAfterWriteNanos(expireAfterWriteNanos);
        }
        if (weigher != null) {
            cache.setWeigher(weigher);
        }
        if (removalListener != null) {
            cache.setRemovalListener(removalListener);
        }
        return cache;
    }
}
