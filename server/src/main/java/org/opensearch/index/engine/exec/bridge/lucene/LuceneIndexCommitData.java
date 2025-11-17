/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.bridge.lucene;

import org.apache.lucene.index.IndexCommit;
import org.opensearch.index.engine.exec.bridge.CommitData;

import java.io.IOException;
import java.util.Map;

/**
 * Lucene implementation of CommitData.
 */
public final class LuceneIndexCommitData implements CommitData {

    private final IndexCommit indexCommit;

    /**
     * Creates a new Lucene commit data wrapper.
     * @param indexCommit the index commit to wrap
     */
    public LuceneIndexCommitData(IndexCommit indexCommit) {
        this.indexCommit = indexCommit;
    }

    /**
     * Gets the user data from the commit.
     * @return the user data map
     * @throws IOException if an I/O error occurs
     */
    @Override
    public Map<String, String> getUserData() throws IOException {
        return indexCommit.getUserData();
    }
}
