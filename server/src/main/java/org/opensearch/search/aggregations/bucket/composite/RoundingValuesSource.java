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
import org.opensearch.common.Rounding;
import org.opensearch.index.fielddata.SortedBinaryDocValues;
import org.opensearch.index.fielddata.SortedNumericDoubleValues;
import org.opensearch.search.aggregations.support.ValuesSource;

import java.io.IOException;

/**
 * A wrapper for {@link ValuesSource.Numeric} that uses {@link Rounding} to transform the long values
 * produced by the underlying source.
 *
 * @opensearch.internal
 */
public class RoundingValuesSource extends ValuesSource.Numeric {
    private final ValuesSource.Numeric vs;
    private final Rounding.Prepared preparedRounding;
    private final Rounding rounding;

    /**
     * @param vs               The original values source
     * @param preparedRounding How to round the values
     * @param rounding         The rounding strategy
     */
    RoundingValuesSource(Numeric vs, Rounding.Prepared preparedRounding, Rounding rounding) {
        this.vs = vs;
        this.preparedRounding = preparedRounding;
        this.rounding = rounding;
    }

    @Override
    public boolean isFloatingPoint() {
        return false;
    }

    @Override
    public boolean isBigInteger() {
        return false;
    }

    public Rounding.Prepared getPreparedRounding() {
        return preparedRounding;
    }

    public Rounding getRounding() {
        return rounding;
    }

    public long round(long value) {
        return preparedRounding.round(value);
    }

    @Override
    public SortedNumericDocValues longValues(LeafReaderContext context) throws IOException {
        SortedNumericDocValues values = vs.longValues(context);
        return new SortedNumericDocValues() {
            @Override
            public long nextValue() throws IOException {
                return round(values.nextValue());
            }

            @Override
            public int docValueCount() {
                return values.docValueCount();
            }

            @Override
            public boolean advanceExact(int target) throws IOException {
                return values.advanceExact(target);
            }

            @Override
            public int docID() {
                return values.docID();
            }

            @Override
            public int nextDoc() throws IOException {
                return values.nextDoc();
            }

            @Override
            public int advance(int target) throws IOException {
                return values.advance(target);
            }

            @Override
            public long cost() {
                return values.cost();
            }
        };
    }

    @Override
    public SortedBinaryDocValues bytesValues(LeafReaderContext context) throws IOException {
        throw new UnsupportedOperationException("not applicable");
    }

    @Override
    public SortedNumericDoubleValues doubleValues(LeafReaderContext context) throws IOException {
        throw new UnsupportedOperationException("not applicable");
    }
}
