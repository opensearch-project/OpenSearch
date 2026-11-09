/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.stream.StreamErrorCode;
import org.opensearch.transport.stream.StreamException;

import java.io.IOException;

/**
 * Tests for {@link FlightUtils#causeSummary}, the one-line replacement for handing a throwable to
 * log4j on the client stream paths. It must stay total (no throw, no recursion) for every shape of
 * throwable those paths can see, because it runs while a stream is already failing.
 */
public class FlightUtilsTests extends OpenSearchTestCase {

    public void testNullThrowable() {
        assertEquals("none", FlightUtils.causeSummary(null));
    }

    public void testSingleThrowableIncludesTypeAndMessage() {
        assertEquals(
            "StreamException[stream gone]",
            FlightUtils.causeSummary(new StreamException(StreamErrorCode.UNAVAILABLE, "stream gone"))
        );
    }

    public void testThrowableWithoutMessageOmitsBrackets() {
        assertEquals("IllegalStateException", FlightUtils.causeSummary(new IllegalStateException()));
    }

    /** The cause chain is what actually identifies a failure, so it must survive the flattening. */
    public void testCauseChainIsRendered() {
        Exception root = new IOException("connection reset");
        Exception wrapper = new StreamException(StreamErrorCode.INTERNAL, "open failed", root);

        assertEquals("StreamException[open failed]; caused by: IOException[connection reset]", FlightUtils.causeSummary(wrapper));
    }

    /** A self-referential cause is legal and must terminate rather than spin. */
    public void testSelfReferentialCauseTerminates() {
        Exception self = new RuntimeException("loops to itself") {
            @Override
            public synchronized Throwable getCause() {
                return this;
            }
        };

        String summary = FlightUtils.causeSummary(self);

        assertTrue("message must still be reported: " + summary, summary.contains("loops to itself"));
        assertFalse("a self-cause must not be walked into: " + summary, summary.contains("caused by"));
    }

    /** A deep chain is truncated rather than emitting an unbounded log line. */
    public void testDeepChainIsTruncated() {
        Exception e = new RuntimeException("depth-0");
        for (int i = 1; i <= 8; i++) {
            e = new RuntimeException("depth-" + i, e);
        }

        String summary = FlightUtils.causeSummary(e);

        assertTrue("deepest included cause must be depth-4: " + summary, summary.contains("depth-4"));
        assertFalse("depth-3 is past the cap and must be elided: " + summary, summary.contains("depth-3"));
        assertTrue("truncation must be visible: " + summary, summary.endsWith("; ..."));
    }
}
