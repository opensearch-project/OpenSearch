/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.commit;

import java.io.IOException;
import java.util.List;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexDeletionPolicy;

public final class LuceneIndexDeletionPolicy extends IndexDeletionPolicy {

    private IndexCommit latestIndexCommit;

    @Override
    public void onInit(List<? extends IndexCommit> commits) throws IOException {

    }

    @Override
    public void onCommit(List<? extends IndexCommit> commits) throws IOException {
        latestIndexCommit = commits.getLast();
    }

    public IndexCommit getLatestIndexCommit() {
        return latestIndexCommit;
    }
}
