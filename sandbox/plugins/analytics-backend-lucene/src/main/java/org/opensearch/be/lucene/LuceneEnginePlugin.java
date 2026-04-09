/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.commit.Committer;
import org.opensearch.index.engine.exec.commit.CommitterConfig;

import java.io.IOException;

/**
 * Static helpers for creating Lucene-based engine components.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
final class LuceneEnginePlugin {

    private LuceneEnginePlugin() {}

    /**
     * Creates a new {@link LuceneCommitter} for the given settings.
     *
     * @param committerSettings the committer settings
     * @return a new committer
     * @throws IOException if committer initialization fails
     */
    static Committer createCommitter(CommitterConfig committerSettings) throws IOException {
        return new LuceneCommitter(committerSettings);
    }
}
