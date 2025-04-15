/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline;

public final class DocIdAtSearchShard {
    public DocIdAtSearchShard(int docId, SearchShard searchShard) {
        this.docId = docId;
        this.searchShard = searchShard;
    }

    public int getDocId() {
        return docId;
    }

    public SearchShard getSearchShard() {
        return searchShard;
    }

    int docId;
    SearchShard searchShard;
}
