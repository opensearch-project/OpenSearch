/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.index.BaseTermsEnum;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.util.BytesRef;

/**
 * A terms enum over exactly one synthesized term, for field types whose value is the same in every document.
 * It has no postings: {@link #docFreq()} reports the number of documents the term is in instead.
 *
 * @opensearch.internal
 */
final class ConstantTermsEnum extends BaseTermsEnum {

    private enum State {
        UNPOSITIONED,
        ON_TERM,
        EXHAUSTED
    }

    private final BytesRef term;
    private final int docFreq;
    private State state = State.UNPOSITIONED;

    ConstantTermsEnum(BytesRef term, int docFreq) {
        this.term = term;
        this.docFreq = docFreq;
    }

    @Override
    public BytesRef next() {
        state = state == State.UNPOSITIONED ? State.ON_TERM : State.EXHAUSTED;
        return state == State.ON_TERM ? term : null;
    }

    @Override
    public SeekStatus seekCeil(BytesRef text) {
        int cmp = term.compareTo(text);
        if (cmp < 0) {
            state = State.EXHAUSTED;
            return SeekStatus.END;
        }
        state = State.ON_TERM;
        return cmp == 0 ? SeekStatus.FOUND : SeekStatus.NOT_FOUND;
    }

    @Override
    public BytesRef term() {
        return state == State.ON_TERM ? term : null;
    }

    @Override
    public int docFreq() {
        return docFreq;
    }

    @Override
    public long totalTermFreq() {
        return docFreq;
    }

    @Override
    public PostingsEnum postings(PostingsEnum reuse, int flags) {
        throw new UnsupportedOperationException("synthesized term");
    }

    @Override
    public ImpactsEnum impacts(int flags) {
        throw new UnsupportedOperationException("synthesized term");
    }

    @Override
    public long ord() {
        throw new UnsupportedOperationException("synthesized term");
    }

    @Override
    public void seekExact(long ord) {
        throw new UnsupportedOperationException("synthesized term");
    }
}
