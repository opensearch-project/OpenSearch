/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.opensearch.search.internal.SearchContext;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * Merges two ScoreDoc arrays into one
 */
class HybridQueryScoreDocsMerger<T extends ScoreDoc> {

    private static final int MIN_NUMBER_OF_ELEMENTS_IN_SCORE_DOC = 3;
    // both magic numbers are randomly generated, there should be no collision as whole part of score is huge
    // and OpenSearch convention is that scores are positive numbers
    public static final Float MAGIC_NUMBER_START_STOP = -9549511920.4881596047f;
    public static final Float MAGIC_NUMBER_DELIMITER = -4422440593.9791198149f;

    public HybridQueryScoreDocsMerger() {}

    /**
     * Merge two score docs objects, result ScoreDocs[] object will have all hits per sub-query from both original objects.
     * Input and output ScoreDocs are in format that is specific to Hybrid Query. This method should not be used for ScoreDocs from
     * other query types.
     * Logic is based on assumption that hits of every sub-query are sorted by score.
     * Method returns new object and doesn't mutate original ScoreDocs arrays.
     * @param sourceScoreDocs original score docs from query result
     * @param newScoreDocs new score docs that we need to merge into existing scores
     * @param comparator comparator to compare the score docs
     * @param isSortEnabled flag that show if sort is enabled or disabled
     * @return merged array of ScoreDocs objects
     */
    public T[] merge(final T[] sourceScoreDocs, final T[] newScoreDocs, final Comparator<T> comparator, final boolean isSortEnabled) {
        if (Objects.requireNonNull(sourceScoreDocs, "score docs cannot be null").length < MIN_NUMBER_OF_ELEMENTS_IN_SCORE_DOC
            || Objects.requireNonNull(newScoreDocs, "score docs cannot be null").length < MIN_NUMBER_OF_ELEMENTS_IN_SCORE_DOC) {
            throw new IllegalArgumentException("cannot merge top docs because it does not have enough elements");
        }
        // we overshoot and preallocate more than we need - length of both top docs combined.
        // we will take only portion of the array at the end
        List<T> mergedScoreDocs = new ArrayList<>(sourceScoreDocs.length + newScoreDocs.length);
        int sourcePointer = 0;
        // mark beginning of hybrid query results by start element
        mergedScoreDocs.add(sourceScoreDocs[sourcePointer]);
        sourcePointer++;
        // new pointer is set to 1 as we don't care about it start-stop element
        int newPointer = 1;

        while (sourcePointer < sourceScoreDocs.length - 1 && newPointer < newScoreDocs.length - 1) {
            // every iteration is for results of one sub-query
            mergedScoreDocs.add(sourceScoreDocs[sourcePointer]);
            sourcePointer++;
            newPointer++;
            // simplest case when both arrays have results for sub-query
            while (sourcePointer < sourceScoreDocs.length
                && isHybridQueryScoreDocElement(sourceScoreDocs[sourcePointer])
                && newPointer < newScoreDocs.length
                && isHybridQueryScoreDocElement(newScoreDocs[newPointer])) {
                if (compareCondition(sourceScoreDocs[sourcePointer], newScoreDocs[newPointer], comparator, isSortEnabled)) {
                    mergedScoreDocs.add(sourceScoreDocs[sourcePointer]);
                    sourcePointer++;
                } else {
                    mergedScoreDocs.add(newScoreDocs[newPointer]);
                    newPointer++;
                }
            }
            // at least one object got exhausted at this point, now merge all elements from object that's left
            while (sourcePointer < sourceScoreDocs.length && isHybridQueryScoreDocElement(sourceScoreDocs[sourcePointer])) {
                mergedScoreDocs.add(sourceScoreDocs[sourcePointer]);
                sourcePointer++;
            }
            while (newPointer < newScoreDocs.length && isHybridQueryScoreDocElement(newScoreDocs[newPointer])) {
                mergedScoreDocs.add(newScoreDocs[newPointer]);
                newPointer++;
            }
        }
        // mark end of hybrid query results by end element
        mergedScoreDocs.add(sourceScoreDocs[sourceScoreDocs.length - 1]);
        if (isSortEnabled) {
            return mergedScoreDocs.toArray((T[]) new FieldDoc[0]);
        }
        return mergedScoreDocs.toArray((T[]) new ScoreDoc[0]);
    }

    private boolean compareCondition(
        final ScoreDoc oldScoreDoc,
        final ScoreDoc secondScoreDoc,
        final Comparator<T> comparator,
        final boolean isSortEnabled
    ) {
        // If sorting is enabled then compare condition will be different then normal HybridQuery
        if (isSortEnabled) {
            return comparator.compare((T) oldScoreDoc, (T) secondScoreDoc) < 0;
        } else {
            return comparator.compare((T) oldScoreDoc, (T) secondScoreDoc) >= 0;
        }
    }

    public static FieldDoc createFieldDocStartStopElementForHybridSearchResults(final int docId, final Object[] fields) {
        return new FieldDoc(docId, MAGIC_NUMBER_START_STOP, fields);
    }

    /**
     * Create ScoreDoc object that is a delimiter element between sub-query results in hybrid search query results
     * @param docId id of one of docs from actual result object, or -1 if there are no matches
     * @return
     */
    public static FieldDoc createFieldDocDelimiterElementForHybridSearchResults(final int docId, final Object[] fields) {
        return new FieldDoc(docId, MAGIC_NUMBER_DELIMITER, fields);
    }

    /**
     * Checking if passed scoreDocs object is a special element (start/stop or delimiter) in the list of hybrid query result scores
     * @param scoreDoc score doc object to check on
     * @return true if it is a special element
     */
    public static boolean isHybridQuerySpecialElement(final ScoreDoc scoreDoc) {
        if (Objects.isNull(scoreDoc)) {
            return false;
        }
        return isHybridQueryStartStopElement(scoreDoc) || isHybridQueryDelimiterElement(scoreDoc);
    }

    /**
     * Checking if passed scoreDocs object is a document score element
     * @param scoreDoc score doc object to check on
     * @return true if element has score
     */
    public static boolean isHybridQueryScoreDocElement(final ScoreDoc scoreDoc) {
        if (Objects.isNull(scoreDoc)) {
            return false;
        }
        return !isHybridQuerySpecialElement(scoreDoc);
    }


    /**
     * Checking if passed scoreDocs object is a start/stop element in the list of hybrid query result scores
     * @param scoreDoc
     * @return true if it is a start/stop element
     */
    public static boolean isHybridQueryStartStopElement(final ScoreDoc scoreDoc) {
        return Objects.nonNull(scoreDoc) && scoreDoc.doc >= 0 && Float.compare(scoreDoc.score, MAGIC_NUMBER_START_STOP) == 0;
    }

    /**
     * Checking if passed scoreDocs object is a delimiter element in the list of hybrid query result scores
     * @param scoreDoc
     * @return true if it is a delimiter element
     */
    public static boolean isHybridQueryDelimiterElement(final ScoreDoc scoreDoc) {
        return Objects.nonNull(scoreDoc) && scoreDoc.doc >= 0 && Float.compare(scoreDoc.score, MAGIC_NUMBER_DELIMITER) == 0;
    }
}
