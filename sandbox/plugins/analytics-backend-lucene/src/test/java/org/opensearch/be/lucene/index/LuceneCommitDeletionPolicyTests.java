/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.index;

import org.apache.lucene.index.IndexCommit;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LuceneCommitDeletionPolicyTests extends OpenSearchTestCase {

    private IndexCommit mockCommit(Map<String, String> userData) throws IOException {
        IndexCommit commit = mock(IndexCommit.class);
        when(commit.getUserData()).thenReturn(userData);
        return commit;
    }

    public void testOnInitWithOnlyNonCSCommitDoesNotDelete() throws IOException {
        LuceneCommitDeletionPolicy policy = new LuceneCommitDeletionPolicy();
        IndexCommit initial = mockCommit(Map.of("history_uuid", "abc"));

        policy.onInit(List.of(initial));

        verify(initial, never()).delete();
    }

    public void testInitialCommitDeletedWhenCSCommitAppears() throws IOException {
        LuceneCommitDeletionPolicy policy = new LuceneCommitDeletionPolicy();
        IndexCommit initial = mockCommit(Map.of("history_uuid", "abc"));
        IndexCommit csCommit = mockCommit(Map.of(CatalogSnapshot.CATALOG_SNAPSHOT_KEY, "blob", CatalogSnapshot.CATALOG_SNAPSHOT_ID, "1"));

        policy.onInit(List.of(initial));
        verify(initial, never()).delete();

        policy.onCommit(List.of(initial, csCommit));
        verify(initial).delete();
    }

    public void testInitialCommitDeletedOnlyOnce() throws IOException {
        LuceneCommitDeletionPolicy policy = new LuceneCommitDeletionPolicy();
        IndexCommit initial = mockCommit(Map.of("history_uuid", "abc"));
        IndexCommit cs1 = mockCommit(Map.of(CatalogSnapshot.CATALOG_SNAPSHOT_KEY, "b1", CatalogSnapshot.CATALOG_SNAPSHOT_ID, "1"));
        IndexCommit cs2 = mockCommit(Map.of(CatalogSnapshot.CATALOG_SNAPSHOT_KEY, "b2", CatalogSnapshot.CATALOG_SNAPSHOT_ID, "2"));

        policy.onInit(List.of(initial));
        policy.onCommit(List.of(initial, cs1));
        policy.onCommit(List.of(initial, cs1, cs2));

        verify(initial).delete(); // exactly once
    }

    public void testPurgeCommitDeletedOnNextOnCommit() throws IOException {
        LuceneCommitDeletionPolicy policy = new LuceneCommitDeletionPolicy();
        IndexCommit csCommit = mockCommit(Map.of(CatalogSnapshot.CATALOG_SNAPSHOT_KEY, "blob", CatalogSnapshot.CATALOG_SNAPSHOT_ID, "42"));

        policy.onCommit(List.of(csCommit));
        verify(csCommit, never()).delete();

        policy.purgeCommit(42L);
        verify(csCommit, never()).delete();

        // onCommit triggers the actual IndexCommit.delete()
        policy.onCommit(List.of(csCommit));
        verify(csCommit).delete();
    }

    public void testPurgeCommitWithUnknownIdIsNoOp() throws IOException {
        LuceneCommitDeletionPolicy policy = new LuceneCommitDeletionPolicy();
        IndexCommit csCommit = mockCommit(Map.of(CatalogSnapshot.CATALOG_SNAPSHOT_KEY, "blob", CatalogSnapshot.CATALOG_SNAPSHOT_ID, "1"));

        policy.onCommit(List.of(csCommit));
        // Unknown IDs are silently skipped (e.g., synthetic initial snapshot never committed to Lucene)
        policy.purgeCommit(999L);
    }
}
