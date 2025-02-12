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

package org.opensearch.script.expression;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A {@link DoubleValuesSource} which has a stub {@link DoubleValues} that holds a dynamically replaceable constant double. This is made
 * thread-safe for concurrent segment search use case by keeping the {@link DoubleValues} per thread. Any update to the value happens in
 * thread specific {@link DoubleValuesSource} instance.
 */
final class PerThreadReplaceableConstDoubleValueSource extends DoubleValuesSource {
    // Multiple slices can be processed by same thread but that will be sequential, so keeping per thread is fine
    final Map<Long, ReplaceableConstDoubleValues> perThreadDoubleValues;

    PerThreadReplaceableConstDoubleValueSource() {
        perThreadDoubleValues = new ConcurrentHashMap<>();
    }

    @Override
    public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
        return perThreadDoubleValues.computeIfAbsent(Thread.currentThread().getId(), threadId -> new ReplaceableConstDoubleValues());
    }

    @Override
    public boolean needsScores() {
        return false;
    }

    @Override
    public Explanation explain(LeafReaderContext ctx, int docId, Explanation scoreExplanation) throws IOException {
        final ReplaceableConstDoubleValues currentFv = perThreadDoubleValues.computeIfAbsent(
            Thread.currentThread().getId(),
            threadId -> new ReplaceableConstDoubleValues()
        );
        if (currentFv.advanceExact(docId)) return Explanation.match((float) currentFv.doubleValue(), "ReplaceableConstDoubleValues");
        else return Explanation.noMatch("ReplaceableConstDoubleValues");
    }

    @Override
    public boolean equals(Object o) {
        return o == this;
    }

    @Override
    public int hashCode() {
        return System.identityHashCode(this);
    }

    public void setValue(double v) {
        final ReplaceableConstDoubleValues currentFv = perThreadDoubleValues.computeIfAbsent(
            Thread.currentThread().getId(),
            threadId -> new ReplaceableConstDoubleValues()
        );
        currentFv.setValue(v);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
        return false;
    }

    @Override
    public DoubleValuesSource rewrite(IndexSearcher reader) {
        return this;
    }
}
