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

import org.apache.lucene.util.BytesRef;
import org.opensearch.common.annotation.PublicApi;

import java.io.IOException;

/**
 * A list of per-document binary values, sorted
 * according to {@link BytesRef#compareTo(BytesRef)}.
 * There might be dups however.
 *
  * @opensearch.api
 */
@PublicApi(since = "1.0.0")
// TODO: Should it expose a count (current approach) or return null when there are no more values?
public abstract class SortedBinaryDocValues {

    /**
     * Advance this instance to the given document id
     * @return true if there is a value for this document
     */
    public abstract boolean advanceExact(int doc) throws IOException;

    /**
     * Retrieves the number of values for the current document.  This must always
     * be greater than zero.
     * It is illegal to call this method after {@link #advanceExact(int)}
     * returned {@code false}.
     */
    public abstract int docValueCount();

    /**
     * Iterates to the next value in the current document. Do not call this more than
     * {@link #docValueCount} times for the document.
     * Note that the returned {@link BytesRef} might be reused across invocations.
     */
    public abstract BytesRef nextValue() throws IOException;

}
