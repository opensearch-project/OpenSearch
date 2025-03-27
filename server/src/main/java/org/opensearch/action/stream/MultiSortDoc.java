/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.stream;

import java.util.Arrays;

public final class MultiSortDoc implements Comparable<MultiSortDoc> {
    private final int docId;
    private final Object[] sortValues;
    private final int segmentOrd;
    private final int[] multipliers;

    public MultiSortDoc(int docId, Object[] sortValues, int segmentOrd, int[] multipliers) {
        this.docId = docId;
        this.sortValues = sortValues;
        this.segmentOrd = segmentOrd;
        this.multipliers = multipliers;
    }

    public int getDocId() {
        return docId;
    }

    public Object[] getSortValues() {
        return sortValues;
    }

    public int getSegmentOrd() {
        return segmentOrd;
    }

    @Override
    public int compareTo(MultiSortDoc other) {
        for (int i = 0; i < sortValues.length; i++) {
            @SuppressWarnings("unchecked")
            Comparable<Object> thisVal = (Comparable<Object>) sortValues[i];
            @SuppressWarnings("unchecked")
            Comparable<Object> otherVal = (Comparable<Object>) other.sortValues[i];
            int cmp = thisVal.compareTo(otherVal);
            cmp *= multipliers[i];
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }

    @Override
    public String toString() {
        return "MultiSortDoc{docId=" + docId + ", sortValues=" + Arrays.toString(sortValues) +
            ", segmentOrd=" + segmentOrd + "}";
    }
}
