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

package org.opensearch.index.mapper;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.lucene.BytesRefs;
import org.opensearch.common.lucene.search.AutomatonQueries;
import org.opensearch.index.query.QueryShardContext;

import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;

/** Base {@link MappedFieldType} implementation for a field that is indexed
 *  with the inverted index.
 *
 * @opensearch.internal
 */
public abstract class TermBasedFieldType extends SimpleMappedFieldType {

    public TermBasedFieldType(
        String name,
        boolean isSearchable,
        boolean isStored,
        boolean hasDocValues,
        TextSearchInfo textSearchInfo,
        Map<String, String> meta
    ) {
        super(name, isSearchable, isStored, hasDocValues, textSearchInfo, meta);
    }

    /** Returns the indexed value used to construct search "values".
     *  This method is used for the default implementations of most
     *  query factory methods such as {@link #termQuery}. */
    protected BytesRef indexedValueForSearch(Object value) {
        return BytesRefs.toBytesRef(value);
    }

    @Override
    public Query termQueryCaseInsensitive(Object value, QueryShardContext context) {
        failIfNotIndexed();
        Query query = AutomatonQueries.caseInsensitiveTermQuery(new Term(name(), indexedValueForSearch(value)));
        if (boost() != 1f) {
            query = new BoostQuery(query, boost());
        }
        return query;
    }

    @Override
    public Query termQuery(Object value, QueryShardContext context) {
        failIfNotIndexed();
        Query query = new TermQuery(new Term(name(), indexedValueForSearch(value)));
        if (boost() != 1f) {
            query = new BoostQuery(query, boost());
        }
        return query;
    }

    @Override
    public Query termsQuery(List<?> values, QueryShardContext context) {
        failIfNotIndexed();
        List<BytesRef> bytesRefs = new ArrayList<>(values.size());
        boolean sorted = true;
        for (int i = 0; i < values.size(); i++) {
            BytesRef elem = indexedValueForSearch(values.get(i));
            if (!bytesRefs.isEmpty()) {
                sorted &= elem.compareTo(bytesRefs.getLast()) >= 0;
            }
            bytesRefs.add(elem);
        }
        return new TermInSetQuery(
            MultiTermQuery.CONSTANT_SCORE_BLENDED_REWRITE,
            name(),
            !sorted ? bytesRefs : new SortedBytesSet(bytesRefs)
        );
    }

    static class SortedBytesSet extends AbstractSet<BytesRef> implements SortedSet<BytesRef> {

        private final List<BytesRef> bytesRefs;

        public SortedBytesSet(List<BytesRef> bytesRefs) {
            this.bytesRefs = bytesRefs;
        }

        @Override
        public Iterator<BytesRef> iterator() {
            return bytesRefs.iterator();
        }

        @Override
        public int size() {
            return bytesRefs.size();
        }

        @Override
        public Comparator<? super BytesRef> comparator() {
            return null;
        }

        /** NSFW */
        @Override
        public SortedSet<BytesRef> subSet(BytesRef fromElement, BytesRef toElement) {
            int fromIdx = Collections.binarySearch(bytesRefs, fromElement);
            int toIdx = Collections.binarySearch(bytesRefs, toElement);
            return new SortedBytesSet(bytesRefs.subList(fromIdx >= 0 ? fromIdx : -fromIdx + 1, toIdx >= 0 ? toIdx : -toIdx + 1));
        }

        /** NSFW */
        @Override
        public SortedSet<BytesRef> headSet(BytesRef toElement) {
            int toIdx = Collections.binarySearch(bytesRefs, toElement);
            return new SortedBytesSet(bytesRefs.subList(0, toIdx >= 0 ? toIdx : -toIdx + 1));
        }

        /** NSFW */
        @Override
        public SortedSet<BytesRef> tailSet(BytesRef fromElement) {
            int fromIdx = Collections.binarySearch(bytesRefs, fromElement);
            return new SortedBytesSet(bytesRefs.subList(fromIdx >= 0 ? fromIdx : -fromIdx + 1, bytesRefs.size()));
        }

        @Override
        public BytesRef first() {
            return bytesRefs.getFirst();
        }

        @Override
        public BytesRef last() {
            return bytesRefs.getLast();
        }

        /** Dedicated for {@link TermInSetQuery#TermInSetQuery(String, Collection)}. */
        @Override
        public <T> T[] toArray(T[] a) {
            return bytesRefs.toArray(a);
        }
    }
}
