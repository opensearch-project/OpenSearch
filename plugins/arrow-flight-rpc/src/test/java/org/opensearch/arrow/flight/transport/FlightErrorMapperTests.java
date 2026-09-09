/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.ErrorFlightMetadata;
import org.apache.arrow.flight.FlightRuntimeException;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.stream.StreamErrorCode;
import org.opensearch.transport.stream.StreamException;

import java.util.List;

import static org.opensearch.OpenSearchException.OPENSEARCH_PREFIX_KEY;
import static org.opensearch.arrow.flight.transport.ClientHeaderMiddleware.CORRELATION_ID_KEY;
import static org.opensearch.arrow.flight.transport.ClientHeaderMiddleware.RAW_HEADER_KEY;

public class FlightErrorMapperTests extends OpenSearchTestCase {

    public void testFromFlightExceptionKeepsOnlyRelevantTrailers() {
        ErrorFlightMetadata metadata = new ErrorFlightMetadata();
        metadata.insert(RAW_HEADER_KEY, "c29tZS1pbnRlcm5hbC1oZWFkZXI=");
        metadata.insert("content-type", "application/grpc");
        metadata.insert(CORRELATION_ID_KEY, "12345");

        FlightRuntimeException flightException = CallStatus.INTERNAL.withDescription("boom").withMetadata(metadata).toRuntimeException();

        StreamException streamException = FlightErrorMapper.fromFlightException(flightException);

        assertEquals(StreamErrorCode.INTERNAL, streamException.getErrorCode());
        // Internal transport trailers are not carried over into the exception metadata.
        assertNull(streamException.getMetadata(OPENSEARCH_PREFIX_KEY + RAW_HEADER_KEY));
        assertNull(streamException.getMetadata(OPENSEARCH_PREFIX_KEY + "content-type"));
        // The allow-listed correlation-id is preserved.
        assertEquals(List.of("12345"), streamException.getMetadata(OPENSEARCH_PREFIX_KEY + CORRELATION_ID_KEY));

        // The rendered exception only reflects the allow-listed trailers.
        String rendered = streamException.toString();
        assertFalse(rendered, rendered.contains(RAW_HEADER_KEY));
        assertFalse(rendered, rendered.contains("application/grpc"));
    }

    /**
     * Round-trip: a StreamException's message + error code must survive toFlightException → back. The
     * immutable-builder bug (CallStatus.withDescription's return value discarded) dropped the description,
     * leaving callers with gRPC's placeholder ("Internal error [task_id=N]") and no message.
     */
    public void testToFlightExceptionPreservesDescriptionAndErrorCode() {
        StreamException original = new StreamException(StreamErrorCode.RESOURCE_EXHAUSTED, "memory budget exhausted on shard");

        FlightRuntimeException flightException = FlightErrorMapper.toFlightException(original);
        StreamException roundTripped = FlightErrorMapper.fromFlightException(flightException);

        assertEquals(StreamErrorCode.RESOURCE_EXHAUSTED, roundTripped.getErrorCode());
        assertTrue(
            "description must survive the round-trip, got: " + roundTripped.getMessage(),
            roundTripped.getMessage() != null && roundTripped.getMessage().contains("memory budget exhausted on shard")
        );
    }

    public void testFromFlightExceptionWithNoMetadata() {
        FlightRuntimeException flightException = CallStatus.UNAVAILABLE.withDescription("unavailable").toRuntimeException();

        StreamException streamException = FlightErrorMapper.fromFlightException(flightException);

        assertEquals(StreamErrorCode.UNAVAILABLE, streamException.getErrorCode());
        assertTrue(streamException.getMetadataKeys().isEmpty());
    }
}
