/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.opensearch.index.engine.exec.commit.Committer;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Optional;

/**
 * Tests for {@link LuceneSearchEnginePlugin}.
 */
public class LuceneSearchEnginePluginTests extends OpenSearchTestCase {

    /**
     * Test that getCommitter() returns a non-empty Optional containing
     * a LuceneCommitter instance.
     *
     * Validates: Requirements 4.2
     */
    public void testGetCommitterReturnsLuceneCommitter() {
        LuceneSearchEnginePlugin plugin = new LuceneSearchEnginePlugin();
        Optional<Committer> committer = plugin.getCommitter(null);

        assertTrue("getCommitter() should return a non-empty Optional", committer.isPresent());
        assertTrue("getCommitter() should return a LuceneCommitter instance", committer.get() instanceof LuceneCommitter);
    }
}
