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

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.util.BytesRef;

/**
 * Specialization of {@link IndexFieldData} for data that is indexed with ordinals.
 *
 * @opensearch.internal
 */
public interface IndexOrdinalsFieldData extends IndexFieldData.Global<LeafOrdinalsFieldData> {

    /**
     * Load a global view of the ordinals for the given {@link IndexReader},
     * potentially from a cache.
     */
    @Override
    IndexOrdinalsFieldData loadGlobal(DirectoryReader indexReader);

    /**
     * Load a global view of the ordinals for the given {@link IndexReader}.
     */
    @Override
    IndexOrdinalsFieldData loadGlobalDirect(DirectoryReader indexReader) throws Exception;

    /**
     * Load a <b>group-scoped</b> global view of the ordinals for the given {@link IndexReader}, restricted to terms
     * that start with {@code termPrefix}. The returned {@link OrdinalMap} is addressable by native segment ordinals
     * for in-range terms, so it can be used directly by the join collectors. This is never cached and is intended for
     * {@code has_child}/{@code has_parent} queries that are already filtered to the same prefix (group).
     * <p>
     * The default implementation ignores the prefix and delegates to {@link #loadGlobalDirect(DirectoryReader)}.
     */
    default IndexOrdinalsFieldData loadGlobalScopedDirect(DirectoryReader indexReader, BytesRef termPrefix) throws Exception {
        return loadGlobalDirect(indexReader);
    }

    /**
     * Load a <b>group-scoped</b> global view of the ordinals for {@code termPrefix}, <b>potentially from a dedicated
     * scoped cache</b> (node-level, separate from the main fielddata cache). This is the cached counterpart of
     * {@link #loadGlobalScopedDirect(DirectoryReader, BytesRef)} and is what {@code has_child}/{@code has_parent}
     * queries should call, so a burst of legs/queries for the same group reuses one build until the next refresh.
     * <p>
     * The default implementation is uncached and delegates to {@link #loadGlobalScopedDirect(DirectoryReader, BytesRef)}.
     */
    default IndexOrdinalsFieldData loadGlobalScoped(DirectoryReader indexReader, BytesRef termPrefix) {
        try {
            return loadGlobalScopedDirect(indexReader, termPrefix);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to build scoped global ordinals", e);
        }
    }

    /**
     * Returns the underlying {@link OrdinalMap} for this fielddata
     * or null if global ordinals are not needed (constant value or single segment).
     */
    OrdinalMap getOrdinalMap();

    /**
     * Whether this field data is able to provide a mapping between global and segment ordinals,
     * by returning the underlying {@link OrdinalMap}. If this method returns false, then calling
     * {@link #getOrdinalMap} will result in an {@link UnsupportedOperationException}.
     */
    boolean supportsGlobalOrdinalsMapping();
}
