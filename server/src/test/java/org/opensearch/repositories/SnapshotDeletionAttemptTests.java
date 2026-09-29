/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories;

import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;

public class SnapshotDeletionAttemptTests extends OpenSearchTestCase {

    private final List<RepositoryData> answered = new ArrayList<>();
    private final List<Exception> failed = new ArrayList<>();
    private final ActionListener<RepositoryData> continuation = ActionListener.wrap(answered::add, failed::add);

    public void testExpiryBeforeTheClaimRefusesTheCommit() {
        final SnapshotDeletionAttempt attempt = new SnapshotDeletionAttempt();
        assertEquals(SnapshotDeletionAttempt.Expiry.NOT_COMMITTED, attempt.expire(continuation));
        assertTrue(attempt.isAbandoned());
        assertFalse("a commit claimed after the expiry must be refused", attempt.claimCommit());
        assertTrue("and the continuation is never called", answered.isEmpty() && failed.isEmpty());
    }

    public void testExpiryWhileTheCommitIsInFlightWaitsForItsOutcome() {
        final SnapshotDeletionAttempt attempt = new SnapshotDeletionAttempt();
        assertTrue(attempt.claimCommit());
        assertFalse(attempt.isAbandoned());
        assertEquals(SnapshotDeletionAttempt.Expiry.PENDING, attempt.expire(continuation));
        assertTrue("an expired attempt admits no new work", attempt.isAbandoned());
        assertTrue("nothing is answered until the commit's outcome is known", answered.isEmpty() && failed.isEmpty());
        final RepositoryData data = RepositoryData.EMPTY.withGenId(7L);
        attempt.committed(data);
        assertEquals("the continuation is answered once, with the committed data", List.of(data), answered);
        assertTrue(failed.isEmpty());
        assertSame(data, attempt.committedRepositoryData());
        assertTrue(attempt.isAbandoned());
    }

    public void testExpiryWhileTheCommitIsInFlightFailsWithTheCommitsOwnFailure() {
        final SnapshotDeletionAttempt attempt = new SnapshotDeletionAttempt();
        assertTrue(attempt.claimCommit());
        assertEquals(SnapshotDeletionAttempt.Expiry.PENDING, attempt.expire(continuation));
        final Exception cause = new RuntimeException("publication failed");
        attempt.commitUnconfirmed(cause);
        assertEquals("the continuation is failed with the commit's own failure", List.of(cause), failed);
        assertTrue(answered.isEmpty());
        assertNull(attempt.committedRepositoryData());
        assertTrue(attempt.isAbandoned());
    }

    public void testExpiryAfterTheCommitAnswersWithTheCommittedData() {
        final SnapshotDeletionAttempt attempt = new SnapshotDeletionAttempt();
        assertTrue(attempt.claimCommit());
        final RepositoryData data = RepositoryData.EMPTY.withGenId(9L);
        attempt.committed(data);
        assertFalse(attempt.isAbandoned());
        assertEquals(SnapshotDeletionAttempt.Expiry.RELEASED, attempt.expire(continuation));
        assertEquals("the continuation is answered before expire returns", List.of(data), answered);
        assertTrue("a released attempt admits no new work", attempt.isAbandoned());
    }

    public void testAnUnconfirmedCommitWithoutAnExpiryAbandonsTheAttempt() {
        final SnapshotDeletionAttempt attempt = new SnapshotDeletionAttempt();
        assertTrue(attempt.claimCommit());
        attempt.commitUnconfirmed(new RuntimeException("publication failed"));
        assertTrue(attempt.isAbandoned());
        assertEquals(SnapshotDeletionAttempt.Expiry.NOT_COMMITTED, attempt.expire(continuation));
        assertTrue(answered.isEmpty() && failed.isEmpty());
    }

    public void testAFailureBeforeTheClaimChangesNothing() {
        final SnapshotDeletionAttempt attempt = new SnapshotDeletionAttempt();
        attempt.commitUnconfirmed(new RuntimeException("the commit task failed before its claim"));
        assertFalse(attempt.isAbandoned());
        assertTrue(attempt.claimCommit());
    }

    public void testASecondExpiryIsRefused() {
        final SnapshotDeletionAttempt attempt = new SnapshotDeletionAttempt();
        assertTrue(attempt.claimCommit());
        assertEquals(SnapshotDeletionAttempt.Expiry.PENDING, attempt.expire(continuation));
        expectThrows(IllegalStateException.class, () -> attempt.expire(continuation));
        attempt.committed(RepositoryData.EMPTY);
        expectThrows(IllegalStateException.class, () -> attempt.expire(continuation));
        assertEquals("the continuation is still answered only once", 1, answered.size());
    }

    public void testTheFirstCleanupFailureIsKept() {
        final SnapshotDeletionAttempt attempt = new SnapshotDeletionAttempt();
        final Exception first = new RuntimeException("first");
        attempt.recordCleanupFailure(first);
        attempt.recordCleanupFailure(new RuntimeException("second"));
        assertSame(first, attempt.cleanupFailure());
    }

    public void testEveryUnbudgetedCallGetsItsOwnAttempt() {
        final SnapshotDeletionAttempt one = SnapshotDeletionAttempt.notAbandoned();
        final SnapshotDeletionAttempt two = SnapshotDeletionAttempt.notAbandoned();
        assertNotSame("a shared instance would let one caller expire another caller's deletion", one, two);
        one.expire(continuation);
        assertFalse(two.isAbandoned());
    }
}
