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

package org.opensearch.search.aggregations.bucket.composite;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.opensearch.index.fielddata.SortedBinaryDocValues;
import org.opensearch.index.fielddata.SortedNumericDoubleValues;
import org.opensearch.search.aggregations.support.ValuesSource;

import java.io.IOException;

/**
 * The values source for histogram agg
 *
 * @opensearch.internal
 */
class HistogramValuesSource extends ValuesSource.Numeric {
    private final Numeric vs;
    private final double interval;

    /**
     *
     * @param vs The original values source
     */
    HistogramValuesSource(Numeric vs, double interval) {
        this.vs = vs;
        this.interval = interval;
    }

    @Override
    public boolean isFloatingPoint() {
        return true;
    }

    @Override
    public SortedNumericDoubleValues doubleValues(LeafReaderContext context) throws IOException {
        SortedNumericDoubleValues values = vs.doubleValues(context);
        return new SortedNumericDoubleValues() {
            @Override
            public double nextValue() throws IOException {
                return Math.floor(values.nextValue() / interval) * interval;
            }

            @Override
            public int docValueCount() {
                return values.docValueCount();
            }

            @Override
            public boolean advanceExact(int target) throws IOException {
                return values.advanceExact(target);
            }
        };
    }

    @Override
    public SortedBinaryDocValues bytesValues(LeafReaderContext context) throws IOException {
        throw new UnsupportedOperationException("not applicable");
    }

    @Override
    public SortedNumericDocValues longValues(LeafReaderContext context) throws IOException {
        throw new UnsupportedOperationException("not applicable");
    }
}
