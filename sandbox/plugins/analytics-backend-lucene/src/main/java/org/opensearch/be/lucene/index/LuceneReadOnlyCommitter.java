/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.index;

import org.opensearch.index.engine.exec.commit.CommitterConfig;

import java.io.IOException;

/**
 * A {@link org.opensearch.index.engine.exec.commit.Committer} that provides read access
 * to committed state but rejects new commits.
 * <p>
 * Extends {@link LuceneReplicaCommitter} to inherit operations that read from the on-disk
 * commit — including reading committed metadata and discovering existing commit points.
 * <p>
 * Overrides {@link #commit(CommitInput)} to throw {@link UnsupportedOperationException}.
 * This is appropriate for any context where commits must not be produced.
 *
 * @opensearch.experimental
 */
public class LuceneReadOnlyCommitter extends LuceneReplicaCommitter {

    public LuceneReadOnlyCommitter(CommitterConfig committerConfig) throws IOException {
        super(committerConfig);
    }

    /** Always throws {@link UnsupportedOperationException}. */
    @Override
    public CommitResult commit(CommitInput commitInput) throws IOException {
        throw new UnsupportedOperationException("LuceneReadOnlyCommitter does not support commit().");
    }
}
