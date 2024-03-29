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

package org.opensearch.core.common.util;

import org.apache.lucene.util.BytesRef;
import org.opensearch.common.annotation.PublicApi;

import java.nio.ByteBuffer;

/**
 * Abstraction of an array of byte values.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public interface ByteArray extends BigArray {

    /**
     * Get an element given its index.
     */
    byte get(long index);

    /**
     * Set a value at the given index and return the previous value.
     */
    byte set(long index, byte value);

    /**
     * Get a reference to a slice.
     *
     * @return <code>true</code> when a byte[] was materialized, <code>false</code> otherwise.
     */
    boolean get(long index, int len, BytesRef ref);

    /**
     * Bulk set.
     */
    void set(long index, byte[] buf, int offset, int len);

    /**
     * Fill slots between <code>fromIndex</code> inclusive to <code>toIndex</code> exclusive with <code>value</code>.
     */
    void fill(long fromIndex, long toIndex, byte value);

    /**
     * Checks if this instance is backed by a single byte array analogous to {@link ByteBuffer#hasArray()}.
     */
    boolean hasArray();

    /**
     * Get backing byte array analogous to {@link ByteBuffer#array()}.
     */
    byte[] array();
}
