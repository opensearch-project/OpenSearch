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
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Compute global distributed frequency across the index
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class AggregatedDfs implements Writeable {

    private Map<Term, TermStatistics> termStatistics;
    private Map<String, CollectionStatistics> fieldStatistics;
    private long maxDoc;

    public AggregatedDfs(StreamInput in) throws IOException {
        int size = in.readVInt();
        final Map<Term, TermStatistics> termStatistics = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            Term term = new Term(in.readString(), in.readBytesRef());
            TermStatistics stats = new TermStatistics(in.readBytesRef(), in.readVLong(), DfsSearchResult.subOne(in.readVLong()));
            termStatistics.put(term, stats);
        }
        this.termStatistics = Collections.unmodifiableMap(termStatistics);
        this.fieldStatistics = DfsSearchResult.readFieldStats(in);
        maxDoc = in.readVLong();
    }

    public AggregatedDfs(
        final Map<Term, TermStatistics> termStatistics,
        final Map<String, CollectionStatistics> fieldStatistics,
        long maxDoc
    ) {
        this.termStatistics = termStatistics;
        this.fieldStatistics = fieldStatistics;
        this.maxDoc = maxDoc;
    }

    public Map<Term, TermStatistics> termStatistics() {
        return termStatistics;
    }

    public Map<String, CollectionStatistics> fieldStatistics() {
        return fieldStatistics;
    }

    public long maxDoc() {
        return maxDoc;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeVInt(termStatistics.size());

        for (final Map.Entry<Term, TermStatistics> c : termStatistics().entrySet()) {
            Term term = c.getKey();
            out.writeString(term.field());
            out.writeBytesRef(term.bytes());
            TermStatistics stats = c.getValue();
            out.writeBytesRef(stats.term());
            out.writeVLong(stats.docFreq());
            out.writeVLong(DfsSearchResult.addOne(stats.totalTermFreq()));
        }

        DfsSearchResult.writeFieldStats(out, fieldStatistics);
        out.writeVLong(maxDoc);
    }
}
