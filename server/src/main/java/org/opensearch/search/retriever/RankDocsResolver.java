/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.Uid;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Resolves a window of {@link RankDoc}s to their Lucene doc ids within a single segment by seeking each
 * {@code _id} in the segment's {@code _id} terms dictionary ({@code TermsEnum.seekExact}). This is the one
 * place the {@code _id -> luceneDocId} translation lives, shared by {@link RankDocsQuery}'s scorer (to
 * attach the pre-computed score) and {@link RankDocsSortField}'s comparator (to attach the position), so the
 * seek is implemented exactly once.
 * <p>
 * {@code _id} is the only identity that is stable across the fresh readers of the final search (a Lucene
 * doc id from an earlier round is meaningless against a different reader), so a per-{@code _id} seek is
 * the correct re-identification mechanism when the retriever tree is not pinned to a point-in-time reader.
 * <p>
 * The result is a compact, ascending-by-docId mapping of the docs that actually resolved in this segment.
 * Docs whose {@code _id} does not resolve (deleted / moved / not on this segment) are omitted, mirroring
 * how a plain {@code _search} drops a doc deleted between its query and fetch phases.
 * <p>
 * Not thread safe; construct and use one per segment on the thread handling that segment.
 *
 * @opensearch.internal
 */
final class RankDocsResolver {

    private RankDocsResolver() {}

    /** A resolved {@code (luceneDocId -> RankDoc)} pair for one segment, sorted ascending by docId. */
    static final class Resolved {
        final int[] docIds;
        final RankDoc[] docs;

        Resolved(int[] docIds, RankDoc[] docs) {
            this.docIds = docIds;
            this.docs = docs;
        }

        int size() {
            return docIds.length;
        }
    }

    /**
     * Resolve {@code window} against {@code ctx}'s segment by seeking each {@code _id}.
     * Only the entries of {@code window} that belong to this segment's reader are resolved; the caller is
     * expected to have already filtered {@code window} to the shard, but any {@code _id} that does not seek
     * is simply skipped.
     *
     * @return docs that resolved in this segment, sorted ascending by Lucene doc id (never null; may be empty)
     */
    static Resolved resolve(LeafReaderContext ctx, List<RankDoc> window) throws IOException {
        if (window.isEmpty()) {
            return new Resolved(new int[0], new RankDoc[0]);
        }
        final Terms terms = ctx.reader().terms(IdFieldMapper.NAME);
        if (terms == null) {
            // Segment carries no _id terms (e.g. an all-no-op segment); nothing resolves here.
            return new Resolved(new int[0], new RankDoc[0]);
        }
        final TermsEnum termsEnum = terms.iterator();
        final Bits liveDocs = ctx.reader().getLiveDocs();

        final List<int[]> docIdIndex = new ArrayList<>(window.size()); // [docId, windowOrdinal]
        PostingsEnum postings = null;
        for (int i = 0; i < window.size(); i++) {
            if (termsEnum.seekExact(Uid.encodeId(window.get(i).id())) == false) {
                continue;
            }
            postings = termsEnum.postings(postings, PostingsEnum.NONE);
            final int docId = firstLiveDoc(postings, liveDocs);
            if (docId != DocIdSetIterator.NO_MORE_DOCS) {
                docIdIndex.add(new int[] { docId, i });
            }
        }

        // DocIdSetIterator requires strictly ascending doc ids: ArrayDocIdSetIterator.advance walks the
        // array forward only, so the scorer would drop or misorder hits if these weren't sorted. The _id
        // seek yields them in window (ranking) order, which is unrelated to Lucene doc-id order.
        docIdIndex.sort(Comparator.comparingInt(a -> a[0]));
        final int[] docIds = new int[docIdIndex.size()];
        final RankDoc[] docs = new RankDoc[docIdIndex.size()];
        for (int i = 0; i < docIdIndex.size(); i++) {
            docIds[i] = docIdIndex.get(i)[0];
            docs[i] = window.get(docIdIndex.get(i)[1]);
        }
        return new Resolved(docIds, docs);
    }

    /** The _id field is a primary key: return the single live doc for the current term, or NO_MORE_DOCS. */
    private static int firstLiveDoc(PostingsEnum postings, Bits liveDocs) throws IOException {
        for (int d = postings.nextDoc(); d != DocIdSetIterator.NO_MORE_DOCS; d = postings.nextDoc()) {
            if (liveDocs == null || liveDocs.get(d)) {
                return d;
            }
        }
        return DocIdSetIterator.NO_MORE_DOCS;
    }
}
