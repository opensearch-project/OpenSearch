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

    public void testSimpleSegrepPrimaryProcessed() {
        long seqNo1, seqNo2;
        assertThat(tracker.getProcessedCheckpoint(), equalTo(SequenceNumbers.NO_OPS_PERFORMED));
        seqNo1 = tracker.generateSeqNo();
        assertThat(seqNo1, equalTo(0L));
        tracker.segrepMarkSeqNoAsProcessed(seqNo1);
        assertThat(tracker.getProcessedCheckpoint(), equalTo(-1L));

        tracker.markSeqNoAsPersisted(seqNo1);
        assertThat(tracker.getPersistedCheckpoint(), equalTo(0L));
        tracker.segrepMarkSeqNoAsProcessed(seqNo1);
        tracker.segrepMarkSeqNoAsProcessed(seqNo1);
        assertThat(tracker.getProcessedCheckpoint(), equalTo(0L));
        assertThat(tracker.hasProcessed(0L), equalTo(true));
        assertThat(tracker.hasProcessed(atLeast(1)), equalTo(false));

        seqNo1 = tracker.generateSeqNo();
        seqNo2 = tracker.generateSeqNo();
        assertThat(seqNo1, equalTo(1L));
        assertThat(seqNo2, equalTo(2L));
        tracker.markSeqNoAsPersisted(seqNo1);
        tracker.markSeqNoAsPersisted(seqNo2);
        assertThat(tracker.getProcessedCheckpoint(), equalTo(0L));
        assertThat(tracker.getPersistedCheckpoint(), equalTo(2L));

        tracker.segrepMarkSeqNoAsProcessed(seqNo2);
        assertThat(tracker.getProcessedCheckpoint(), equalTo(2L));
        assertThat(tracker.hasProcessed(seqNo1), equalTo(true));
        assertThat(tracker.hasProcessed(seqNo2), equalTo(true));

        tracker.segrepMarkSeqNoAsProcessed(seqNo1);
        assertThat(tracker.getProcessedCheckpoint(), equalTo(2L));
        assertThat(tracker.hasProcessed(between(0, 2)), equalTo(true));
        assertThat(tracker.hasProcessed(atLeast(3)), equalTo(false));
        assertThat(tracker.getMaxSeqNo(), equalTo(2L));
    }
}
