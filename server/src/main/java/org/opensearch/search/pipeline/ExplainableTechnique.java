/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline;

import java.util.List;
import java.util.Map;

/**
 * Abstracts explanation of score combination or normalization technique.
 */
public interface ExplainableTechnique {

    String GENERIC_DESCRIPTION_OF_TECHNIQUE = "generic score processing technique";

    /**
     * Returns a string with general description of the technique
     */
    default String describe() {
        return GENERIC_DESCRIPTION_OF_TECHNIQUE;
    }

    /**
     * Returns a map with explanation for each document id
     * @param queryTopDocs collection of CompoundTopDocs for each shard result
     * @return map of document per shard and corresponding explanation object
     */
    default Map<DocIdAtSearchShard, ExplanationDetails> explain(final List<CompoundTopDocs> queryTopDocs) {
        return Map.of();
    }
}
