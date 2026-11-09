/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat.merge;

import org.opensearch.OpenSearchException;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

/**
 * Tests for {@link MergeFailedEngineException}.
 */
public class MergeFailedEngineExceptionTests extends OpenSearchTestCase {

    public void testExceptionMessageAndCause() {
        ShardId shardId = new ShardId(new Index("test-index", "uuid"), 0);
        IOException cause = new IOException("disk full");

        MergeFailedEngineException exception = new MergeFailedEngineException(shardId, cause);

        assertSame(cause, exception.getCause());
        assertTrue(exception.getMessage().contains("Merge failed"));
        assertEquals(shardId, exception.getShardId());
        assertTrue(exception instanceof OpenSearchException);
    }
}
