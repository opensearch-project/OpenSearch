/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.termsenum;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Set;

/**
 * Simulates document-level security in tests: every document whose {@code id} term is in {@code hiddenIds} is
 * removed from the live docs, exactly as a security reader wrapper hides documents.
 */
public final class HidingDirectoryReader extends FilterDirectoryReader {

    private final Set<String> hiddenIds;
    private final boolean lowerNumDocs;

    /** Hides the documents and lowers {@code numDocs()} to match, as the Lucene reader contract requires. */
    public HidingDirectoryReader(DirectoryReader in, Set<String> hiddenIds) throws IOException {
        this(in, hiddenIds, true);
    }

    /**
     * Hides the documents through the live docs only. When {@code lowerNumDocs} is false, {@code numDocs()} still
     * counts the hidden documents, which simulates a reader wrapper that does not follow the Lucene contract.
     */
    public HidingDirectoryReader(DirectoryReader in, Set<String> hiddenIds, boolean lowerNumDocs) throws IOException {
        super(in, new HidingSubReaderWrapper(hiddenIds, lowerNumDocs));
        this.hiddenIds = hiddenIds;
        this.lowerNumDocs = lowerNumDocs;
    }

    @Override
    protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
        return new HidingDirectoryReader(in, hiddenIds, lowerNumDocs);
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return in.getReaderCacheHelper();
    }

    private static final class HidingSubReaderWrapper extends SubReaderWrapper {
        private final Set<String> hiddenIds;
        private final boolean lowerNumDocs;

        HidingSubReaderWrapper(Set<String> hiddenIds, boolean lowerNumDocs) {
            this.hiddenIds = hiddenIds;
            this.lowerNumDocs = lowerNumDocs;
        }

        @Override
        public LeafReader wrap(LeafReader reader) {
            try {
                return new HidingLeafReader(reader, hiddenIds, lowerNumDocs);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private static final class HidingLeafReader extends FilterLeafReader {
        private final FixedBitSet liveDocs;
        private final int numDocs;

        HidingLeafReader(LeafReader in, Set<String> hiddenIds, boolean lowerNumDocs) throws IOException {
            super(in);
            FixedBitSet bits = new FixedBitSet(in.maxDoc());
            Bits inLiveDocs = in.getLiveDocs();
            for (int doc = 0; doc < in.maxDoc(); doc++) {
                if (inLiveDocs == null || inLiveDocs.get(doc)) {
                    bits.set(doc);
                }
            }
            Terms ids = in.terms("id");
            if (ids != null) {
                TermsEnum idsEnum = ids.iterator();
                PostingsEnum postings = null;
                for (String id : hiddenIds) {
                    if (idsEnum.seekExact(new BytesRef(id))) {
                        postings = idsEnum.postings(postings, PostingsEnum.NONE);
                        for (int doc = postings.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = postings.nextDoc()) {
                            bits.clear(doc);
                        }
                    }
                }
            }
            this.liveDocs = bits;
            this.numDocs = lowerNumDocs ? bits.cardinality() : in.numDocs();
        }

        @Override
        public Bits getLiveDocs() {
            return liveDocs;
        }

        @Override
        public int numDocs() {
            return numDocs;
        }

        @Override
        public CacheHelper getCoreCacheHelper() {
            return in.getCoreCacheHelper();
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }
    }
}
