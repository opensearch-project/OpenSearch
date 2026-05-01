/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.index;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.commit.Committer;
import org.opensearch.index.engine.exec.commit.CommitterConfig;
import org.opensearch.index.engine.exec.commit.CommitterFactory;

import java.io.IOException;

/**
 * {@link CommitterFactory} implementation that creates {@link LuceneCommitter} instances.
 * <p>
 * Registered by {@link org.opensearch.be.lucene.LucenePlugin} via the
 * {@link org.opensearch.plugins.EnginePlugin} SPI. When the composite engine initializes
 * a shard, it calls {@link #getCommitter(CommitterConfig)} to obtain a committer that owns
 * the shared Lucene {@link org.apache.lucene.index.IndexWriter} for durable segment commits.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class LuceneCommitterFactory implements CommitterFactory {

    /** Creates a new factory instance. */
    public LuceneCommitterFactory() {}

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
