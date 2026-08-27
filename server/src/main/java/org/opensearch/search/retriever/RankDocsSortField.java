/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.SortField;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * A synthetic, query-scoped {@link SortField} that orders documents by their retriever-assigned
 * {@code position} whenever the retriever's desired order is decoupled from {@code _score} — i.e. the
 * final ranking cannot be reproduced by sorting on score. Pinning a document to a fixed rank is one such
 * case; others include reranked results whose new order no longer tracks the original scores, or any
 * retriever that assigns an explicit position independent of the score it reports.
 * <p>
 * The value being sorted on — {@code position} — does not exist in the index; it is a per-request number
 * the coordinator computed. The only identity stable across the fresh readers of the final
 * search is {@code _id}, so the comparator resolves the window's {@code _id}s to <em>this segment's</em>
 * Lucene doc ids <b>once</b> in {@link FieldComparator#getLeafComparator(LeafReaderContext)} — building a
 * {@code docId -> position} lookup via {@link RankDocsResolver} — after which every comparison is an
 * O(1) array lookup. This is deliberately independent of {@link RankDocsQuery}'s own {@code _id} seek:
 * the two share no shard-side channel and have no ordering dependency, so the sort is correct regardless
 * of how Lucene schedules the query scorer.
 * <p>
 * Documents that are not part of the window sort <em>after</em> all positioned docs (sentinel position
 * {@link #UNPINNED_POSITION}).
 *
 * @opensearch.internal
 */
public final class RankDocsSortField extends SortField {

    /** Field name is synthetic; there is no such field in the index. */
    public static final String NAME = "_rank_docs";

    /** Position assigned to any doc not present in the retriever window (sorts last). */
    static final int UNPINNED_POSITION = Integer.MAX_VALUE;

    private final List<RankDoc> rankDocs;
    private final String indexName;
    private final int shardId;

    public RankDocsSortField(List<RankDoc> rankDocs, String indexName, int shardId) {
        super(NAME, SortField.Type.CUSTOM);
        // Already scoped to this shard and made immutable by RankDocsSortBuilder#build; store the reference.
        this.rankDocs = Objects.requireNonNull(rankDocs, "rankDocs");
        this.indexName = Objects.requireNonNull(indexName, "indexName");
        this.shardId = shardId;
    }

    @Override
    public FieldComparator<?> getComparator(int numHits, Pruning pruning) {
        return new FieldComparator<Integer>() {
            private final int[] values = new int[numHits];
            private int bottom;
            private int topValue;

            @Override
            public int compare(int slot1, int slot2) {
                return Integer.compare(values[slot1], values[slot2]);
            }

            @Override
            public void setTopValue(Integer value) {
                this.topValue = value;
            }

            @Override
            public Integer value(int slot) {
                return values[slot];
            }

            @Override
            public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
                // Resolve the window to this segment's doc ids ONCE, then serve O(1) lookups from an
                // O(window)-sized map (NOT an O(maxDoc) dense array): the window (10s-100s) is tiny next
                // to maxDoc (millions), so a dense array would cost O(maxDoc) alloc+fill on every query.
                final RankDocsResolver.Resolved resolved = RankDocsResolver.resolve(context, rankDocs);
                final DocIdToPosition positions = new DocIdToPosition(resolved.size());
                for (int i = 0; i < resolved.size(); i++) {
                    positions.put(resolved.docIds[i], resolved.docs[i].position());
                }

                return new LeafFieldComparator() {
                    @Override
                    public void setScorer(Scorable scorer) {}

                    @Override
                    public void setBottom(int slot) {
                        bottom = values[slot];
                    }

                    @Override
                    public int compareBottom(int doc) {
                        return Integer.compare(bottom, positions.getOrDefault(doc));
                    }

                    @Override
                    public void copy(int slot, int doc) {
                        values[slot] = positions.getOrDefault(doc);
                    }

                    @Override
                    public int compareTop(int doc) {
                        return Integer.compare(topValue, positions.getOrDefault(doc));
                    }
                };
            }
        };
    }

    // SortField.equals for a CUSTOM field ignores our window, so two RankDocsSortFields with different
    // windows would compare equal — override so distinct rankings are never treated as the same sort.
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RankDocsSortField other = (RankDocsSortField) o;
        return shardId == other.shardId && indexName.equals(other.indexName) && rankDocs.equals(other.rankDocs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rankDocs, indexName, shardId);
    }

    /**
     * A minimal primitive {@code int -> int} open-addressing hash map sized to the window, returning
     * {@link #UNPINNED_POSITION} for absent keys. Avoids the boxing of {@code HashMap<Integer,Integer>}
     * and the O(maxDoc) footprint of a dense array — footprint and build cost are O(window).
     */
    static final class DocIdToPosition {
        private final int[] keys;
        private final int[] vals;
        private final int mask;
        private static final int EMPTY = -1;

        DocIdToPosition(int expected) {
            // Next power of two >= expected / 0.6 (load factor ~0.6), min 16.
            int cap = 16;
            final int target = Math.max(1, (int) (expected / 0.6f) + 1);
            while (cap < target) {
                cap <<= 1;
            }
            this.keys = new int[cap];
            this.vals = new int[cap];
            this.mask = cap - 1;
            java.util.Arrays.fill(keys, EMPTY); // O(window), not O(maxDoc)
        }

        void put(int key, int value) {
            int i = hash(key) & mask;
            while (keys[i] != EMPTY && keys[i] != key) {
                i = (i + 1) & mask;
            }
            keys[i] = key;
            vals[i] = value;
        }

        int getOrDefault(int key) {
            int i = hash(key) & mask;
            while (keys[i] != EMPTY) {
                if (keys[i] == key) {
                    return vals[i];
                }
                i = (i + 1) & mask;
            }
            return UNPINNED_POSITION;
        }

        private static int hash(int key) {
            // Fibonacci-style mixing to spread sequential doc ids.
            int h = key * 0x9E3779B1;
            return h ^ (h >>> 16);
        }
    }
}
