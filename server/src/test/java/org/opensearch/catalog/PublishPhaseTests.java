/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.catalog;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

public class PublishPhaseTests extends OpenSearchTestCase {

    public void testOrdinalStability() {
        // Binary wire format is ordinal-based; reordering the enum breaks on-the-wire compat.
        assertEquals(0, PublishPhase.INITIALIZED.ordinal());
        assertEquals(1, PublishPhase.PUBLISHING.ordinal());
        assertEquals(2, PublishPhase.FINALIZING_SUCCESS.ordinal());
        assertEquals(3, PublishPhase.FINALIZING_FAILURE.ordinal());
        assertEquals(4, PublishPhase.FAILED.ordinal());
        assertEquals(5, PublishPhase.values().length);
    }

    public void testWriteableRoundTripForEveryValue() throws Exception {
        for (PublishPhase phase : PublishPhase.values()) {
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                phase.writeTo(out);
                try (StreamInput in = out.bytes().streamInput()) {
                    PublishPhase read = PublishPhase.readFrom(in);
                    assertEquals(phase, read);
                }
            }
        }
    }

    public void testReadFromRejectsUnknownOrdinal() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(99);
            try (StreamInput in = out.bytes().streamInput()) {
                expectThrows(java.io.IOException.class, () -> PublishPhase.readFrom(in));
            }
        }
    }
}
