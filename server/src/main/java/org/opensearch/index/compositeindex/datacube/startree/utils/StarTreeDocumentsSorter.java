/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.utils;

import org.apache.lucene.util.IntroSorter;

import java.util.Objects;
import java.util.function.IntFunction;

/**
 * Utility class for building star tree
 */
public class StarTreeDocumentsSorter {
    /**
     * Sort documents based on the dimension values off heap using intro sorter.
     */
    public static void sort(
        final int[] sortedDocIds,
        final int dimensionId,
        final int numDocs,
        final IntFunction<Long[]> dimensionsReader
    ) {
        new IntroSorter() {
            private Long[] dimensions;

            @Override
            protected void swap(int i, int j) {
                int temp = sortedDocIds[i];
                sortedDocIds[i] = sortedDocIds[j];
                sortedDocIds[j] = temp;
            }

            @Override
            protected void setPivot(int i) {
                dimensions = dimensionsReader.apply(i);
            }

            @Override
            protected int comparePivot(int j) {
                Long[] currentDimensions = dimensionsReader.apply(j);
                for (int i = dimensionId + 1; i < dimensions.length; i++) {
                    Long dimension = currentDimensions[i];
                    if (!Objects.equals(dimensions[i], dimension)) {
                        if (dimensions[i] == null && dimension == null) {
                            return 0;
                        }
                        if (dimension == null) {
                            return -1;
                        }
                        if (dimensions[i] == null) {
                            return 1;
                        }
                        return Long.compare(dimensions[i], dimension);
                    }
                }
                return 0;
            }
        }.sort(0, numDocs);
    }
}
