/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.opensearch.index.engine.exec.commit.Committer;
import org.opensearch.index.engine.exec.commit.CommitterSettings;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Tests for the optional Committer field on {@link DataFormatAwareEngine}.
 */
public class DataFormatAwareEngineCommitterTests extends OpenSearchTestCase {

    /**
     * Verifies that getCommitter() returns null when no committer has been set.
     * Validates: Requirements 4.3, 4.4
     */
    public void testGetCommitterReturnsNullByDefault() {
        DataFormatAwareEngine engine = new DataFormatAwareEngine(new HashMap<>());
        assertNull("getCommitter() should return null by default", engine.getCommitter());
    }

    /**
     * Verifies that setCommitter() followed by getCommitter() returns the same instance.
     * Validates: Requirements 4.3, 4.4
     */
    public void testSetCommitterThenGetCommitterReturnsSameInstance() {
        DataFormatAwareEngine engine = new DataFormatAwareEngine(new HashMap<>());
        Committer committer = new Committer() {
            @Override
            public void init(CommitterSettings settings) throws IOException {}

            @Override
            public void commit(Map<String, String> commitData) throws IOException {}

            @Override
            public void close() throws IOException {}

            @Override
            public Map<String, String> getLastCommittedData() {
                return Map.of();
            }

            @Override
            public CommitStats getCommitStats() {
                return null;
            }

            @Override
            public SafeCommitInfo getSafeCommitInfo() {
                return SafeCommitInfo.EMPTY;
            }
        };

        engine.setCommitter(committer);
        assertSame("getCommitter() should return the exact instance that was set", committer, engine.getCommitter());
    }
}
