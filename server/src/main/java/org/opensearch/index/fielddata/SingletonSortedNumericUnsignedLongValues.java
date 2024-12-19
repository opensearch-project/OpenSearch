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

package org.opensearch.index.fielddata;

import org.apache.lucene.index.SortedNumericDocValues;

import java.io.IOException;

/**
 * Exposes multi-valued view over a single-valued instance.
 * <p>
 * This can be used if you want to have one multi-valued implementation
 * that works for single or multi-valued types.
 *
 * @opensearch.internal
 */

final class SingletonSortedNumericUnsignedLongValues extends SortedNumericUnsignedLongValues {
    private final SortedNumericDocValues values;

    SingletonSortedNumericUnsignedLongValues(SortedNumericDocValues values) {
        this.values = values;
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
        return values.advanceExact(target);
    }

    @Override
    public long nextValue() throws IOException {
        return values.nextValue();
    }

    @Override
    public int docValueCount() {
        return values.docValueCount();
    }

    public int advance(int target) throws IOException {
        return values.advance(target);
    }

    public int docID() {
        return values.docID();
    }

    /** Return the wrapped values. */
    public SortedNumericDocValues getNumericUnsignedLongValues() {
        return values;
    }
}
