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

package org.apache.lucene.search.grouping;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TotalHits;
import org.opensearch.index.mapper.MappedFieldType;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.apache.lucene.search.SortField.Type.SCORE;

/**
 * A collector that groups documents based on field values and returns {@link CollapseTopFieldDocs}
 * output. The collapsing is done in a single pass by selecting only the top sorted document per collapse key.
 * The value used for the collapse key of each group can be found in {@link CollapseTopFieldDocs#collapseValues}.
 * <p>
 * When the sort is descending relevance, the collector propagates the minimum competitive score once
 * {@code orderedGroups} is full and the total-hits threshold is exceeded. This is safe for collapsing
 * since the group {@code sort} is the same as the query sort.
 */
public final class CollapsingTopDocsCollector<T> extends FirstPassGroupingCollector<T> {
    protected final String collapseField;

    protected final Sort sort;
    protected Scorable scorer;

    private int totalHitCount;
    private final FieldDoc after;
    private FieldComparator<?> afterComparator;
    private LeafFieldComparator leafComparator;
    private final int reverseMul;

    private final boolean canSetMinScore;
    private final int totalHitsThreshold;
    private final Map<T, Float> groupBestScores;
    private final float[] groupScores;
    private float minCompetitiveScore;
    private TotalHits.Relation totalHitsRelation;
    private int docBase;

    CollapsingTopDocsCollector(GroupSelector<T> groupSelector, String collapseField, Sort sort, int topN) {
        this(groupSelector, collapseField, sort, topN, null, Integer.MAX_VALUE);
    }

    CollapsingTopDocsCollector(GroupSelector<T> groupSelector, String collapseField, Sort sort, int topN, int totalHitsThreshold) {
        this(groupSelector, collapseField, sort, topN, null, totalHitsThreshold);
    }

    CollapsingTopDocsCollector(GroupSelector<T> groupSelector, String collapseField, Sort sort, int topN, FieldDoc after) {
        this(groupSelector, collapseField, sort, topN, after, Integer.MAX_VALUE);
    }

    CollapsingTopDocsCollector(
        GroupSelector<T> groupSelector,
        String collapseField,
        Sort sort,
        int topN,
        FieldDoc after,
        int totalHitsThreshold
    ) {
        super(groupSelector, sort, topN);
        this.collapseField = collapseField;
        this.sort = sort;
        this.after = after;
        this.totalHitsThreshold = Math.max(0, totalHitsThreshold);

        if (after != null) {
            // we should have only one sort field which is the collapse field
            if (sort.getSort().length != 1 || !sort.getSort()[0].getField().equals(collapseField)) {
                throw new IllegalArgumentException("The after parameter can only be used when the sort is based on the collapse field");
            }
            SortField field = sort.getSort()[0];
            afterComparator = field.getComparator(1, Pruning.NONE);

            @SuppressWarnings("unchecked")
            FieldComparator<Object> comparator = (FieldComparator<Object>) afterComparator;
            comparator.setTopValue(after.fields[0]);

            reverseMul = field.getReverse() ? -1 : 1;
        } else {
            reverseMul = 1;
        }

        // Match Lucene TopFieldCollector: min-score pruning is only enabled for descending
        // relevance when the hit count is allowed to be a lower bound.
        this.canSetMinScore = canSetMinScore(sort) && this.totalHitsThreshold != Integer.MAX_VALUE;
        this.groupBestScores = this.canSetMinScore ? new HashMap<>() : null;
        this.groupScores = this.canSetMinScore ? new float[topN + 1] : null;
        if (this.groupScores != null) {
            Arrays.fill(this.groupScores, Float.NaN);
        }
        this.minCompetitiveScore = 0f;
        this.totalHitsRelation = TotalHits.Relation.EQUAL_TO;
    }

    /**
     * Transform {@link FirstPassGroupingCollector#getTopGroups(int)} output in
     * {@link CollapseTopFieldDocs}. The collapsing needs only one pass so we can get the final top docs at the end
     * of the first pass.
     */
    public CollapseTopFieldDocs getTopDocs() throws IOException {
        Collection<SearchGroup<T>> groups = super.getTopGroups(0);
        if (groups == null) {
            // For search_after, use totalHitCount to preserve hit information
            // For non-search_after, totalHitCount equals 0 when no matches, so behavior unchanged
            TotalHits totalHits = new TotalHits(totalHitCount, totalHitsRelation);
            return new CollapseTopFieldDocs(collapseField, totalHits, new ScoreDoc[0], sort.getSort(), new Object[0]);
        }
        FieldDoc[] docs = new FieldDoc[groups.size()];
        Object[] collapseValues = new Object[groups.size()];
        int scorePos = -1;
        for (int index = 0; index < sort.getSort().length; index++) {
            SortField sortField = sort.getSort()[index];
            if (sortField.getType() == SCORE) {
                scorePos = index;
                break;
            }
        }
        int pos = 0;
        Iterator<CollectedSearchGroup<T>> it = orderedGroups.iterator();
        for (SearchGroup<T> group : groups) {
            assert it.hasNext();
            CollectedSearchGroup<T> col = it.next();
            float score = Float.NaN;
            if (scorePos != -1) {
                score = (float) group.sortValues[scorePos];
            }
            docs[pos] = new FieldDoc(col.topDoc, score, group.sortValues);
            collapseValues[pos] = group.groupValue;
            pos++;
        }
        TotalHits totalHits = new TotalHits(totalHitCount, totalHitsRelation);
        return new CollapseTopFieldDocs(collapseField, totalHits, docs, sort.getSort(), collapseValues);
    }

    @Override
    public ScoreMode scoreMode() {
        if (canSetMinScore) {
            return ScoreMode.TOP_SCORES;
        }
        if (super.scoreMode().needsScores()) {
            return ScoreMode.COMPLETE;
        }
        return ScoreMode.COMPLETE_NO_SCORES;
    }

    @Override
    public void setScorer(Scorable scorer) throws IOException {
        super.setScorer(scorer);
        this.scorer = scorer;
        this.minCompetitiveScore = 0f;
        // Re-apply the threshold for this leaf, matching Lucene's TopFieldCollector.
        maybeUpdateMinCompetitiveScore();
    }

    @Override
    public void collect(int doc) throws IOException {
        if (after != null && !isAfterDoc(doc)) {
            totalHitCount++;
            return;
        }

        final boolean heapWasEmpty = orderedGroups == null;
        super.collect(doc);
        totalHitCount++;

        if (canSetMinScore == false) {
            return;
        }

        if (heapWasEmpty) {
            recordGroupBestScore();
            if (orderedGroups != null) {
                seedGroupScores();
                maybeUpdateMinCompetitiveScore();
            }
        } else if (updateGroupScore(doc)) {
            maybeUpdateMinCompetitiveScore();
        }
    }

    private void recordGroupBestScore() throws IOException {
        final T groupValue = getGroupSelector().copyValue();
        final float score = scorer.score();
        final Float current = groupBestScores.get(groupValue);
        if (current == null || score > current) {
            groupBestScores.put(groupValue, score);
        }
    }

    private void seedGroupScores() {
        for (CollectedSearchGroup<T> group : orderedGroups) {
            final Float score = groupBestScores.get(group.groupValue);
            if (score != null) {
                groupScores[group.comparatorSlot] = score;
            }
        }
        groupBestScores.clear();
    }

    private boolean updateGroupScore(int doc) throws IOException {
        final int globalDoc = docBase + doc;
        for (CollectedSearchGroup<T> group : orderedGroups) {
            if (group.topDoc == globalDoc) {
                groupScores[group.comparatorSlot] = scorer.score();
                return true;
            }
        }
        return false;
    }

    private void maybeUpdateMinCompetitiveScore() throws IOException {
        if (canSetMinScore && orderedGroups != null && totalHitCount > totalHitsThreshold) {
            updateMinCompetitiveScore();
        }
    }

    private void updateMinCompetitiveScore() throws IOException {
        final CollectedSearchGroup<T> bottomGroup = orderedGroups.last();
        final float minScore = groupScores[bottomGroup.comparatorSlot];
        // Skip if the bottom group's score has not been observed yet: a stale or missing value
        // could be higher than the true bottom and would prune competitive hits.
        if (Float.isNaN(minScore) == false && minScore > minCompetitiveScore) {
            scorer.setMinCompetitiveScore(minScore);
            minCompetitiveScore = minScore;
            totalHitsRelation = TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
        }
    }

    private static boolean canSetMinScore(Sort sort) {
        final SortField[] fields = sort.getSort();
        return fields.length > 0 && fields[0].getType() == SCORE && fields[0].getReverse() == false;
    }

    private boolean isAfterDoc(int doc) throws IOException {
        if (leafComparator == null) return true;

        int cmp = reverseMul * leafComparator.compareTop(doc);
        if (cmp != 0) {
            return cmp < 0;
        }

        return doc > after.doc;
    }

    @Override
    protected void doSetNextReader(LeafReaderContext readerContext) throws IOException {
        super.doSetNextReader(readerContext);
        this.docBase = readerContext.docBase;
        if (after != null) {
            leafComparator = afterComparator.getLeafComparator(readerContext);
        }
    }

    /**
     * Create a collapsing top docs collector on a {@link org.apache.lucene.index.NumericDocValues} field.
     * It accepts also {@link org.apache.lucene.index.SortedNumericDocValues} field but
     * the collect will fail with an {@link IllegalStateException} if a document contains more than one value for the
     * field.
     *
     * @param collapseField     The sort field used to group documents.
     * @param collapseFieldType The {@link MappedFieldType} for this sort field.
     * @param sort              The {@link Sort} used to sort the collapsed hits.
     *                          The collapsing keeps only the top sorted document per collapsed key.
     *                          This must be non-null, ie, if you want to groupSort by relevance
     *                          use Sort.RELEVANCE.
     * @param topN              How many top groups to keep.
     */
    public static CollapsingTopDocsCollector<?> createNumeric(
        String collapseField,
        MappedFieldType collapseFieldType,
        Sort sort,
        int topN
    ) {
        return createNumeric(collapseField, collapseFieldType, sort, topN, Integer.MAX_VALUE);
    }

    /**
     * Create a collapsing top docs collector on a {@link org.apache.lucene.index.NumericDocValues} field.
     * It accepts also {@link org.apache.lucene.index.SortedNumericDocValues} field but
     * the collect will fail with an {@link IllegalStateException} if a document contains more than one value for the
     * field.
     *
     * @param collapseField      The sort field used to group documents.
     * @param collapseFieldType  The {@link MappedFieldType} for this sort field.
     * @param sort               The {@link Sort} used to sort the collapsed hits.
     *                           The collapsing keeps only the top sorted document per collapsed key.
     *                           This must be non-null, ie, if you want to groupSort by relevance
     *                           use Sort.RELEVANCE.
     * @param topN               How many top groups to keep.
     * @param totalHitsThreshold The total hit count up to which an accurate count is required.
     *                           Once exceeded the collector may set a minimum competitive score.
     */
    public static CollapsingTopDocsCollector<?> createNumeric(
        String collapseField,
        MappedFieldType collapseFieldType,
        Sort sort,
        int topN,
        int totalHitsThreshold
    ) {
        return new CollapsingTopDocsCollector<>(
            new CollapsingDocValuesSource.Numeric(collapseFieldType),
            collapseField,
            sort,
            topN,
            totalHitsThreshold
        );
    }

    /**
     * Create a collapsing top docs collector on a {@link org.apache.lucene.index.NumericDocValues} field.
     * It accepts also {@link org.apache.lucene.index.SortedNumericDocValues} field but
     * the collect will fail with an {@link IllegalStateException} if a document contains more than one value for the
     * field.
     *
     * @param collapseField     The sort field used to group documents.
     * @param collapseFieldType The {@link MappedFieldType} for this sort field.
     * @param sort              The {@link Sort} used to sort the collapsed hits.
     *                          The collapsing keeps only the top sorted document per collapsed key.
     *                          This must be non-null, ie, if you want to groupSort by relevance
     *                          use Sort.RELEVANCE.
     * @param topN              How many top groups to keep.
     * @param after             The last sort value of the previous page. Pass null if this is the first page.
     */
    public static CollapsingTopDocsCollector<?> createNumeric(
        String collapseField,
        MappedFieldType collapseFieldType,
        Sort sort,
        int topN,
        FieldDoc after
    ) {
        return createNumeric(collapseField, collapseFieldType, sort, topN, after, Integer.MAX_VALUE);
    }

    /**
     * Create a collapsing top docs collector on a {@link org.apache.lucene.index.NumericDocValues} field.
     * It accepts also {@link org.apache.lucene.index.SortedNumericDocValues} field but
     * the collect will fail with an {@link IllegalStateException} if a document contains more than one value for the
     * field.
     *
     * @param collapseField      The sort field used to group documents.
     * @param collapseFieldType  The {@link MappedFieldType} for this sort field.
     * @param sort               The {@link Sort} used to sort the collapsed hits.
     *                           The collapsing keeps only the top sorted document per collapsed key.
     *                           This must be non-null, ie, if you want to groupSort by relevance
     *                           use Sort.RELEVANCE.
     * @param topN               How many top groups to keep.
     * @param after              The last sort value of the previous page. Pass null if this is the first page.
     * @param totalHitsThreshold The total hit count up to which an accurate count is required.
     *                           Once exceeded the collector may set a minimum competitive score.
     */
    public static CollapsingTopDocsCollector<?> createNumeric(
        String collapseField,
        MappedFieldType collapseFieldType,
        Sort sort,
        int topN,
        FieldDoc after,
        int totalHitsThreshold
    ) {
        return new CollapsingTopDocsCollector<>(
            new CollapsingDocValuesSource.Numeric(collapseFieldType),
            collapseField,
            sort,
            topN,
            after,
            totalHitsThreshold
        );
    }

    /**
     * Create a collapsing top docs collector on a {@link org.apache.lucene.index.SortedDocValues} field.
     * It accepts also {@link org.apache.lucene.index.SortedSetDocValues} field but
     * the collect will fail with an {@link IllegalStateException} if a document contains more than one value for the
     * field.
     *
     * @param collapseField     The sort field used to group documents.
     * @param collapseFieldType The {@link MappedFieldType} for this sort field.
     * @param sort              The {@link Sort} used to sort the collapsed hits. The collapsing keeps only the top sorted
     *                          document per collapsed key.
     *                          This must be non-null, ie, if you want to groupSort by relevance use Sort.RELEVANCE.
     * @param topN              How many top groups to keep.
     */
    public static CollapsingTopDocsCollector<?> createKeyword(
        String collapseField,
        MappedFieldType collapseFieldType,
        Sort sort,
        int topN
    ) {
        return createKeyword(collapseField, collapseFieldType, sort, topN, Integer.MAX_VALUE);
    }

    /**
     * Create a collapsing top docs collector on a {@link org.apache.lucene.index.SortedDocValues} field.
     * It accepts also {@link org.apache.lucene.index.SortedSetDocValues} field but
     * the collect will fail with an {@link IllegalStateException} if a document contains more than one value for the
     * field.
     *
     * @param collapseField      The sort field used to group documents.
     * @param collapseFieldType  The {@link MappedFieldType} for this sort field.
     * @param sort               The {@link Sort} used to sort the collapsed hits. The collapsing keeps only the top sorted
     *                           document per collapsed key.
     *                           This must be non-null, ie, if you want to groupSort by relevance use Sort.RELEVANCE.
     * @param topN               How many top groups to keep.
     * @param totalHitsThreshold The total hit count up to which an accurate count is required.
     *                           Once exceeded the collector may set a minimum competitive score.
     */
    public static CollapsingTopDocsCollector<?> createKeyword(
        String collapseField,
        MappedFieldType collapseFieldType,
        Sort sort,
        int topN,
        int totalHitsThreshold
    ) {
        return new CollapsingTopDocsCollector<>(
            new CollapsingDocValuesSource.Keyword(collapseFieldType),
            collapseField,
            sort,
            topN,
            totalHitsThreshold
        );
    }

    /**
     * Create a collapsing top docs collector on a {@link org.apache.lucene.index.SortedDocValues} field.
     * It accepts also {@link org.apache.lucene.index.SortedSetDocValues} field but
     * the collect will fail with an {@link IllegalStateException} if a document contains more than one value for the
     * field.
     *
     * @param collapseField     The sort field used to group documents.
     * @param collapseFieldType The {@link MappedFieldType} for this sort field.
     * @param sort              The {@link Sort} used to sort the collapsed hits. The collapsing keeps only the top sorted
     *                          document per collapsed key.
     *                          This must be non-null, ie, if you want to groupSort by relevance use Sort.RELEVANCE.
     * @param topN              How many top groups to keep.
     * @param after             The last sort value of the previous page. Pass null if this is the first page.
     */
    public static CollapsingTopDocsCollector<?> createKeyword(
        String collapseField,
        MappedFieldType collapseFieldType,
        Sort sort,
        int topN,
        FieldDoc after
    ) {
        return createKeyword(collapseField, collapseFieldType, sort, topN, after, Integer.MAX_VALUE);
    }

    /**
     * Create a collapsing top docs collector on a {@link org.apache.lucene.index.SortedDocValues} field.
     * It accepts also {@link org.apache.lucene.index.SortedSetDocValues} field but
     * the collect will fail with an {@link IllegalStateException} if a document contains more than one value for the
     * field.
     *
     * @param collapseField      The sort field used to group documents.
     * @param collapseFieldType  The {@link MappedFieldType} for this sort field.
     * @param sort               The {@link Sort} used to sort the collapsed hits. The collapsing keeps only the top sorted
     *                           document per collapsed key.
     *                           This must be non-null, ie, if you want to groupSort by relevance use Sort.RELEVANCE.
     * @param topN               How many top groups to keep.
     * @param after              The last sort value of the previous page. Pass null if this is the first page.
     * @param totalHitsThreshold The total hit count up to which an accurate count is required.
     *                           Once exceeded the collector may set a minimum competitive score.
     */
    public static CollapsingTopDocsCollector<?> createKeyword(
        String collapseField,
        MappedFieldType collapseFieldType,
        Sort sort,
        int topN,
        FieldDoc after,
        int totalHitsThreshold
    ) {
        return new CollapsingTopDocsCollector<>(
            new CollapsingDocValuesSource.Keyword(collapseFieldType),
            collapseField,
            sort,
            topN,
            after,
            totalHitsThreshold
        );
    }
}
