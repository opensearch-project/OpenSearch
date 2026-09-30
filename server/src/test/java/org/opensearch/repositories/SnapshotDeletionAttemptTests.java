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

    public void testExpiryBeforeTheClaimRefusesTheCommit() {
        final List<Object> heard = new ArrayList<>();
        final SnapshotDeletionAttempt attempt = new SnapshotDeletionAttempt();
        assertEquals(SnapshotDeletionAttempt.Expiry.NOT_COMMITTED, attempt.expire(ActionListener.wrap(heard::add, heard::add)));
        assertTrue("an expiry before the claim abandons the attempt", attempt.isAbandoned());
        assertFalse("an expiry refuses a later claim", attempt.claimCommit());
        assertEquals("the continuation is never called", List.of(), heard);
    }

    public void testAnExpiryIsAnsweredWithTheCommitsOutcome() {
        final RepositoryData data = RepositoryData.EMPTY.withGenId(7L);
        final Exception cause = new RuntimeException("publication failed");
        for (boolean expiredFirst : new boolean[] { true, false }) {
            for (boolean confirmed : new boolean[] { true, false }) {
                final String outcome = confirmed ? "commit" : "unconfirmed commit";
                final String row = expiredFirst ? "expiry, then " + outcome : outcome + ", then expiry";
                final List<RepositoryData> answered = new ArrayList<>();
                final List<Exception> failed = new ArrayList<>();
                final ActionListener<RepositoryData> continuation = ActionListener.wrap(answered::add, failed::add);
                final SnapshotDeletionAttempt attempt = new SnapshotDeletionAttempt();
                assertTrue(row, attempt.claimCommit());
                assertFalse(row, attempt.isAbandoned());
                final SnapshotDeletionAttempt.Expiry expiry;
                if (expiredFirst) {
                    expiry = attempt.expire(continuation);
                    assertTrue(row + ": an expired attempt admits no new work", attempt.isAbandoned());
                    assertTrue(row + ": nothing is answered before the commit's outcome", answered.isEmpty() && failed.isEmpty());
                    if (confirmed) {
                        attempt.committed(data);
                    } else {
                        attempt.commitUnconfirmed(cause);
                    }
                } else {
                    if (confirmed) {
                        attempt.committed(data);
                    } else {
                        attempt.commitUnconfirmed(cause);
                    }
                    assertEquals(row + ": only an unconfirmed commit abandons the attempt", confirmed == false, attempt.isAbandoned());
                    expiry = attempt.expire(continuation);
                }
                assertEquals(
                    row,
                    expiredFirst ? SnapshotDeletionAttempt.Expiry.PENDING
                        : confirmed ? SnapshotDeletionAttempt.Expiry.RELEASED
                        : SnapshotDeletionAttempt.Expiry.NOT_COMMITTED,
                    expiry
                );
                assertEquals(row + ": answered once, with the committed data", confirmed ? List.of(data) : List.of(), answered);
                assertEquals(
                    row + ": failed only while waiting, with the commit's own failure",
                    expiredFirst && confirmed == false ? List.of(cause) : List.of(),
                    failed
                );
                assertSame(row, confirmed ? data : null, attempt.committedRepositoryData());
                assertTrue(row + ": an expired attempt admits no new work", attempt.isAbandoned());
            }
        }
    }
}
