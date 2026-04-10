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
import org.opensearch.index.engine.exec.commit.CommitterFactory;

import java.io.IOException;

/**
 * Factory for creating Lucene-based engine components.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
final class LuceneCommitterFactory implements CommitterFactory {

    LuceneCommitterFactory() {}

    /**
     * Creates a new {@link LuceneCommitter} for the given settings.
     *
     * @param committerConfig the committer config
     * @return a new committer
     * @throws IOException if committer initialization fails
     */
    public Committer getCommitter(CommitterConfig committerConfig) throws IOException {
        return new LuceneCommitter(committerConfig);
    }
}
