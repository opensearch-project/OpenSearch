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

package org.opensearch.search.suggest.phrase;

import org.apache.lucene.codecs.TermStats;
import org.apache.lucene.util.BytesRef;
import org.opensearch.search.suggest.phrase.DirectCandidateGenerator.Candidate;
import org.opensearch.search.suggest.phrase.DirectCandidateGenerator.CandidateSet;

import java.io.IOException;

//TODO public for tests

/**
 * Base class for phrase candidates
 *
 * @opensearch.internal
 */
public abstract class CandidateGenerator {

    public abstract boolean isKnownWord(BytesRef term) throws IOException;

    public abstract TermStats termStats(BytesRef term) throws IOException;

    public CandidateSet drawCandidates(BytesRef term) throws IOException {
        CandidateSet set = new CandidateSet(Candidate.EMPTY, createCandidate(term, true));
        return drawCandidates(set);
    }

    public Candidate createCandidate(BytesRef term, boolean userInput) throws IOException {
        return createCandidate(term, termStats(term), 1.0, userInput);
    }

    public Candidate createCandidate(BytesRef term, TermStats termStats, double channelScore) throws IOException {
        return createCandidate(term, termStats, channelScore, false);
    }

    public abstract Candidate createCandidate(BytesRef term, TermStats termStats, double channelScore, boolean userInput)
        throws IOException;

    public abstract CandidateSet drawCandidates(CandidateSet set) throws IOException;
}
