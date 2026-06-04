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

package org.opensearch.search.dfs;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.search.internal.ShardSearchRequest;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Result from a Distributed Frequency Search phase
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class DfsSearchResult extends SearchPhaseResult {

    private static final Term[] EMPTY_TERMS = new Term[0];
    private static final TermStatistics[] EMPTY_TERM_STATS = new TermStatistics[0];
    private Term[] terms;
    private TermStatistics[] termStatistics;
    private Map<String, CollectionStatistics> fieldStatistics;
    private int maxDoc;

    public DfsSearchResult(StreamInput in) throws IOException {
        super(in);
        contextId = new ShardSearchContextId(in);
        int termsSize = in.readVInt();
        if (termsSize == 0) {
            terms = EMPTY_TERMS;
        } else {
            terms = new Term[termsSize];
            for (int i = 0; i < terms.length; i++) {
                terms[i] = new Term(in.readString(), in.readBytesRef());
            }
        }
        this.termStatistics = readTermStats(in, terms);
        this.fieldStatistics = readFieldStats(in);

        maxDoc = in.readVInt();
        setShardSearchRequest(in.readOptionalWriteable(ShardSearchRequest::new));
    }

    public DfsSearchResult(ShardSearchContextId contextId, SearchShardTarget shardTarget, ShardSearchRequest shardSearchRequest) {
        this.setSearchShardTarget(shardTarget);
        this.contextId = contextId;
        setShardSearchRequest(shardSearchRequest);
    }

    public DfsSearchResult maxDoc(int maxDoc) {
        this.maxDoc = maxDoc;
        return this;
    }

    public int maxDoc() {
        return maxDoc;
    }

    public DfsSearchResult termsStatistics(Term[] terms, TermStatistics[] termStatistics) {
        this.terms = terms;
        this.termStatistics = termStatistics;
        return this;
    }

    public DfsSearchResult fieldStatistics(final Map<String, CollectionStatistics> fieldStatistics) {
        this.fieldStatistics = Collections.unmodifiableMap(fieldStatistics);
        return this;
    }

    public Term[] terms() {
        return terms;
    }

    public TermStatistics[] termStatistics() {
        return termStatistics;
    }

    public Map<String, CollectionStatistics> fieldStatistics() {
        return fieldStatistics;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        contextId.writeTo(out);
        out.writeVInt(terms.length);
        for (Term term : terms) {
            out.writeString(term.field());
            out.writeBytesRef(term.bytes());
        }
        writeTermStats(out, termStatistics);
        writeFieldStats(out, fieldStatistics);
        out.writeVInt(maxDoc);
        out.writeOptionalWriteable(getShardSearchRequest());
    }

    public static void writeFieldStats(StreamOutput out, final Map<String, CollectionStatistics> fieldStatistics) throws IOException {
        out.writeVInt(fieldStatistics.size());

        for (final Map.Entry<String, CollectionStatistics> c : fieldStatistics.entrySet()) {
            out.writeString(c.getKey());
            CollectionStatistics statistics = c.getValue();
            assert statistics.maxDoc() >= 0;
            out.writeVLong(statistics.maxDoc());
            // stats are always positive numbers
            out.writeVLong(statistics.docCount());
            out.writeVLong(statistics.sumTotalTermFreq());
            out.writeVLong(statistics.sumDocFreq());
        }
    }

    public static void writeTermStats(StreamOutput out, TermStatistics[] termStatistics) throws IOException {
        out.writeVInt(termStatistics.length);
        for (TermStatistics termStatistic : termStatistics) {
            writeSingleTermStats(out, termStatistic);
        }
    }

    public static void writeSingleTermStats(StreamOutput out, TermStatistics termStatistic) throws IOException {
        if (termStatistic != null) {
            assert termStatistic.docFreq() > 0;
            out.writeVLong(termStatistic.docFreq());
            out.writeVLong(addOne(termStatistic.totalTermFreq()));
        } else {
            out.writeVLong(0);
            out.writeVLong(0);
        }
    }

    static Map<String, CollectionStatistics> readFieldStats(StreamInput in) throws IOException {
        final int numFieldStatistics = in.readVInt();
        final Map<String, CollectionStatistics> fieldStatistics = new HashMap<>(numFieldStatistics);
        for (int i = 0; i < numFieldStatistics; i++) {
            final String field = in.readString();
            assert field != null;
            final long maxDoc = in.readVLong();
            final long docCount;
            final long sumTotalTermFreq;
            final long sumDocFreq;
            // stats are always positive numbers
            docCount = in.readVLong();
            sumTotalTermFreq = in.readVLong();
            sumDocFreq = in.readVLong();
            CollectionStatistics stats = new CollectionStatistics(field, maxDoc, docCount, sumTotalTermFreq, sumDocFreq);
            fieldStatistics.put(field, stats);
        }
        return Collections.unmodifiableMap(fieldStatistics);
    }

    static TermStatistics[] readTermStats(StreamInput in, Term[] terms) throws IOException {
        int termsStatsSize = in.readVInt();
        final TermStatistics[] termStatistics;
        if (termsStatsSize == 0) {
            termStatistics = EMPTY_TERM_STATS;
        } else {
            termStatistics = new TermStatistics[termsStatsSize];
            assert terms.length == termsStatsSize;
            for (int i = 0; i < termStatistics.length; i++) {
                BytesRef term = terms[i].bytes();
                final long docFreq = in.readVLong();
                assert docFreq >= 0;
                final long totalTermFreq = subOne(in.readVLong());
                if (docFreq == 0) {
                    continue;
                }
                termStatistics[i] = new TermStatistics(term, docFreq, totalTermFreq);
            }
        }
        return termStatistics;
    }

    /*
     * optional statistics are set to -1 in lucene by default.
     * Since we are using var longs to encode values we add one to each value
     * to ensure we don't waste space and don't add negative values.
     */
    public static long addOne(long value) {
        assert value + 1 >= 0;
        return value + 1;
    }

    /*
     * See #addOne this just subtracting one and asserts that the actual value
     * is positive.
     */
    public static long subOne(long value) {
        assert value >= 0;
        return value - 1;
    }
}
