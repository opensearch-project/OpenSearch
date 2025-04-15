/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline;

import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

public final class ExplanationDetails {
    public int getDocId() {
        return docId;
    }

    public List<Pair<Float, String>> getScoreDetails() {
        return scoreDetails;
    }

    int docId;

    List<Pair<Float, String>> scoreDetails;

    public ExplanationDetails(List<Pair<Float, String>> scoreDetails) {
        // pass docId as -1 to match docId in SearchHit
        // https://github.com/opensearch-project/OpenSearch/blob/main/server/src/main/java/org/opensearch/search/SearchHit.java#L170
        this(-1, scoreDetails);
    }

    public ExplanationDetails(int docId, List<Pair<Float, String>> scoreDetails) {
        this.docId = docId;
        this.scoreDetails = scoreDetails;
    }
}
