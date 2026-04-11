/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.opensearch.index.engine.exec.commit.Committer;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

/**
 * Tests for the optional Committer field on {@link DataFormatAwareEngine}.
 */
public class DataFormatAwareEngineCommitterTests extends OpenSearchTestCase {

    public void testGetCommitterReturnsNullByDefault() {
        DataFormatAwareEngine engine = new DataFormatAwareEngine(new HashMap<>());
        assertNull(engine.getCommitter());
    }

    public void testSetCommitterThenGetCommitterReturnsSameInstance() {
        DataFormatAwareEngine engine = new DataFormatAwareEngine(new HashMap<>());
        Committer committer = new Committer() {
            @Override
            public void commit(Map<String, String> commitData) {}

            @Override
            public void close() {}

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

            @Override
            public java.util.List<org.opensearch.index.engine.exec.coord.CatalogSnapshot> listCommittedSnapshots() {
                return java.util.List.of();
            }

            @Override
            public void deleteCommit(org.opensearch.index.engine.exec.coord.CatalogSnapshot snapshot) {}
        };
        engine.setCommitter(committer);
        assertSame(committer, engine.getCommitter());
    }
}
