/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.termsenum;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.Nullable;
import org.opensearch.index.mapper.MappedFieldType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BooleanSupplier;

/**
 * Collects the terms of one reader that at least one live document contains, in the order of a field type's
 * {@link MappedFieldType#termsEnum} candidates. Dropping terms without a live document keeps terms of deleted
 * documents, and of documents that a reader wrapper hides for document-level security, out of the result.
 * It stops at {@code size} terms, and returns the terms collected so far when the deadline passes.
 * <p>
 * Liveness is decided from each segment's own {@link org.apache.lucene.index.LeafReader#getLiveDocs()}, never from
 * {@code numDocs()} or {@code hasDeletions()}, so a reader wrapper that hides documents without lowering its
 * document counts still cannot leak their terms.
 *
 * @opensearch.internal
 */
public final class TermsEnumCollector {

    /** How many candidate terms are visited between two deadline checks. */
    static final int CHECK_INTERVAL = 128;
    /** How many postings of a single term are walked between two deadline checks. */
    static final int POSTINGS_CHECK_INTERVAL = 4096;

    /**
     * The terms collected from one reader.
     *
     * @param terms the display strings of the live terms, in candidate order
     * @param timedOut whether the deadline stopped the collection before it was complete
     */
    public record Result(List<String> terms, boolean timedOut) {
        /** Creates a result, copying {@code terms}. */
        public Result {
            terms = List.copyOf(terms);
        }
    }

    private enum Liveness {
        LIVE,
        DEAD,
        TIMED_OUT
    }

    private final int maxDoc;
    /** The first top-level doc id of each segment. */
    private final int[] docBases;
    /** One past the last top-level doc id of each segment. */
    private final int[] docEnds;
    /** Each segment's own live docs, null where the segment neither deletes nor hides a document. */
    private final Bits[] segmentLiveDocs;
    /** Whether no segment deletes or hides a document. */
    private final boolean allSegmentsLive;
    private final BooleanSupplier timedOut;
    private PostingsEnum postings;
    /** Cached answer for terms in every document, which only depends on the reader. */
    @Nullable
    private Liveness termInEveryDocLiveness;

    private TermsEnumCollector(IndexReader reader, BooleanSupplier timedOut) {
        List<LeafReaderContext> leaves = reader.leaves();
        this.maxDoc = reader.maxDoc();
        this.docBases = new int[leaves.size()];
        this.docEnds = new int[leaves.size()];
        this.segmentLiveDocs = new Bits[leaves.size()];
        boolean allLive = true;
        for (int i = 0; i < leaves.size(); i++) {
            LeafReaderContext leaf = leaves.get(i);
            docBases[i] = leaf.docBase;
            docEnds[i] = leaf.docBase + leaf.reader().maxDoc();
            segmentLiveDocs[i] = leaf.reader().getLiveDocs();
            allLive &= segmentLiveDocs[i] == null;
        }
        this.allSegmentsLive = allLive;
        this.timedOut = timedOut;
    }

    /**
     * Collects up to {@code size} candidate terms that sort after {@code searchAfter} and that at least one live
     * document of {@code reader} contains.
     *
     * @param fieldType the field type that produced {@code candidates}, used to render each term
     * @param candidates the candidate terms in ascending byte order, or {@code null} for none
     * @param reader the top-level reader that {@code candidates} enumerates
     * @param searchAfter only terms strictly greater than this are collected, or {@code null} for no lower bound
     * @param size the maximum number of terms to collect, at least 1
     * @param timedOut polled periodically; once it returns {@code true}, the terms collected so far are returned.
     *                 Any exception it throws, such as a {@code TaskCancelledException}, propagates.
     * @return the collected terms and whether the deadline cut the collection short
     * @throws IOException if reading the index fails
     */
    public static Result collect(
        MappedFieldType fieldType,
        @Nullable TermsEnum candidates,
        IndexReader reader,
        @Nullable BytesRef searchAfter,
        int size,
        BooleanSupplier timedOut
    ) throws IOException {
        if (size < 1) {
            throw new IllegalArgumentException("size must be at least 1 but was [" + size + "]");
        }
        if (candidates == null) {
            return new Result(List.of(), false);
        }
        return new TermsEnumCollector(reader, timedOut).collect(fieldType, candidates, searchAfter, size);
    }

    private Result collect(MappedFieldType fieldType, TermsEnum candidates, @Nullable BytesRef searchAfter, int size) throws IOException {
        List<String> terms = new ArrayList<>();
        long visited = 0;
        for (BytesRef term = candidates.next(); term != null; term = candidates.next()) {
            if (searchAfter == null || term.compareTo(searchAfter) > 0) {
                Liveness liveness = liveness(candidates);
                if (liveness == Liveness.TIMED_OUT) {
                    return new Result(terms, true);
                }
                if (liveness == Liveness.LIVE) {
                    terms.add(displayString(fieldType, term));
                    if (terms.size() == size) {
                        return new Result(terms, false);
                    }
                }
            }
            // candidates skipped as dead or not after searchAfter count toward the deadline check too
            if (++visited % CHECK_INTERVAL == 0 && timedOut.getAsBoolean()) {
                return new Result(terms, true);
            }
        }
        return new Result(terms, false);
    }

    /** Whether a live document contains the current term of {@code candidates}. */
    private Liveness liveness(TermsEnum candidates) throws IOException {
        // a term in every document, including a synthesized one that has no postings, is live if any document is
        if (candidates.docFreq() == maxDoc) {
            if (termInEveryDocLiveness != null) {
                return termInEveryDocLiveness;
            }
            Liveness liveness = anyLiveDoc();
            if (liveness != Liveness.TIMED_OUT) {
                termInEveryDocLiveness = liveness;
            }
            return liveness;
        }
        // the term has a posting in some segment, and no segment deletes or hides a document
        if (allSegmentsLive) {
            return Liveness.LIVE;
        }
        // the postings use top-level doc ids, ascending, so each doc is checked against its own segment's live docs
        postings = candidates.postings(postings, PostingsEnum.NONE);
        int segment = -1;
        int docBase = 0;
        int docEnd = 0;
        Bits liveDocs = null;
        long walked = 0;
        for (int doc = postings.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = postings.nextDoc()) {
            while (doc >= docEnd) {
                segment++;
                docBase = docBases[segment];
                docEnd = docEnds[segment];
                liveDocs = segmentLiveDocs[segment];
            }
            // a segment without live docs bits keeps every document, so its first posting is live
            if (liveDocs == null || liveDocs.get(doc - docBase)) {
                return Liveness.LIVE;
            }
            if (++walked % POSTINGS_CHECK_INTERVAL == 0 && timedOut.getAsBoolean()) {
                return Liveness.TIMED_OUT;
            }
        }
        return Liveness.DEAD;
    }

    /**
     * Whether any segment has a live document. This is the per-segment walk for a term that is in every document:
     * its postings in each segment are every doc id, so the segment's live docs are walked directly. A synthesized
     * term has no postings to walk.
     */
    private Liveness anyLiveDoc() {
        long walked = 0;
        for (int segment = 0; segment < segmentLiveDocs.length; segment++) {
            int segmentMaxDoc = docEnds[segment] - docBases[segment];
            Bits liveDocs = segmentLiveDocs[segment];
            if (liveDocs == null && segmentMaxDoc > 0) {
                return Liveness.LIVE;
            }
            for (int doc = 0; doc < segmentMaxDoc; doc++) {
                if (liveDocs.get(doc)) {
                    return Liveness.LIVE;
                }
                if (++walked % POSTINGS_CHECK_INTERVAL == 0 && timedOut.getAsBoolean()) {
                    return Liveness.TIMED_OUT;
                }
            }
        }
        return Liveness.DEAD;
    }

    /**
     * Renders a term the way the field type displays its values.
     *
     * @param fieldType the field type the term belongs to
     * @param term the indexed term
     * @return {@code fieldType.valueForDisplay(term)} as a string; a {@link BytesRef} is decoded as UTF-8
     */
    public static String displayString(MappedFieldType fieldType, BytesRef term) {
        Object value = fieldType.valueForDisplay(term);
        return value instanceof BytesRef bytes ? bytes.utf8ToString() : value.toString();
    }
}
