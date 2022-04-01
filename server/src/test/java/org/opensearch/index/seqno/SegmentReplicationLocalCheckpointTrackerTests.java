/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.seqno;

import org.junit.Before;
import org.opensearch.test.OpenSearchTestCase;

import static org.hamcrest.Matchers.equalTo;

public class SegmentReplicationLocalCheckpointTrackerTests extends OpenSearchTestCase {

    private LocalCheckpointTracker tracker;

    public static LocalCheckpointTracker createEmptyTracker() {
        return new LocalCheckpointTracker(SequenceNumbers.NO_OPS_PERFORMED, SequenceNumbers.NO_OPS_PERFORMED);
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        tracker = createEmptyTracker();
    }

    public void testSimpleSegrepProcessedNoPersistentUpdate() {
        // base case with no persistent checkpoint update
        long seqNo1;
        assertThat(tracker.getProcessedCheckpoint(), equalTo(SequenceNumbers.NO_OPS_PERFORMED));
        seqNo1 = tracker.generateSeqNo();
        assertThat(seqNo1, equalTo(0L));
        tracker.fastForwardProcessedSeqNo(seqNo1);
        assertThat(tracker.getProcessedCheckpoint(), equalTo(-1L));
    }

    public void testSimpleSegrepProcessedPersistentUpdate() {
        // base case with persistent checkpoint update
        long seqNo1;
        assertThat(tracker.getProcessedCheckpoint(), equalTo(SequenceNumbers.NO_OPS_PERFORMED));
        seqNo1 = tracker.generateSeqNo();
        assertThat(seqNo1, equalTo(0L));

        tracker.markSeqNoAsPersisted(seqNo1);
        assertThat(tracker.getPersistedCheckpoint(), equalTo(0L));
        tracker.fastForwardProcessedSeqNo(seqNo1);
        assertThat(tracker.getProcessedCheckpoint(), equalTo(0L));
        assertThat(tracker.hasProcessed(0L), equalTo(true));
        assertThat(tracker.hasProcessed(atLeast(1)), equalTo(false));

        // idempotent case
        tracker.fastForwardProcessedSeqNo(seqNo1);
        assertThat(tracker.getProcessedCheckpoint(), equalTo(0L));
        assertThat(tracker.hasProcessed(0L), equalTo(true));
        assertThat(tracker.hasProcessed(atLeast(1)), equalTo(false));

    }

    public void testSimpleSegrepProcessedPersistentUpdate2() {
        long seqNo1, seqNo2;
        assertThat(tracker.getProcessedCheckpoint(), equalTo(SequenceNumbers.NO_OPS_PERFORMED));
        seqNo1 = tracker.generateSeqNo();
        seqNo2 = tracker.generateSeqNo();
        assertThat(seqNo1, equalTo(0L));
        assertThat(seqNo2, equalTo(1L));
        tracker.markSeqNoAsPersisted(seqNo1);
        tracker.markSeqNoAsPersisted(seqNo2);
        assertThat(tracker.getProcessedCheckpoint(), equalTo(-1L));
        assertThat(tracker.getPersistedCheckpoint(), equalTo(1L));

        tracker.fastForwardProcessedSeqNo(seqNo2);
        assertThat(tracker.getProcessedCheckpoint(), equalTo(1L));
        assertThat(tracker.hasProcessed(seqNo1), equalTo(true));
        assertThat(tracker.hasProcessed(seqNo2), equalTo(true));

        tracker.fastForwardProcessedSeqNo(seqNo1);
        assertThat(tracker.getProcessedCheckpoint(), equalTo(1L));
        assertThat(tracker.hasProcessed(between(0, 1)), equalTo(true));
        assertThat(tracker.hasProcessed(atLeast(2)), equalTo(false));
        assertThat(tracker.getMaxSeqNo(), equalTo(1L));
    }
}
