/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.startree;

import java.util.List;

public interface DimensionFilterMerger<P extends DimensionFilter, Q extends DimensionFilter> {

    List<DimensionFilter> merge(P left, Q right);

    enum SingletonFactory {

        EXACT_WITH_EXACT(new DimensionFilterMerger<ExactMatchDimFilter, ExactMatchDimFilter>() {
            @Override
            public List<DimensionFilter> merge(ExactMatchDimFilter left, ExactMatchDimFilter right) {
                return null;
            }
        });

        private final DimensionFilterMerger<? extends DimensionFilter, ? extends DimensionFilter> mergeAlgorithm;

        SingletonFactory(DimensionFilterMerger<? extends DimensionFilter, ? extends DimensionFilter> mergeAlgorithm) {
            this.mergeAlgorithm = mergeAlgorithm;
        }

        public DimensionFilterMerger<? extends DimensionFilter, ? extends DimensionFilter> getMergeAlgorithm() {
            return mergeAlgorithm;
        }

    }

}
