/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.Version;
import org.opensearch.arrow.flight.stream.ArrowStreamInput;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.transport.Header;
import org.opensearch.transport.InboundDecoder;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportStatus;
import org.opensearch.transport.stream.StreamTransportResponse;

import java.io.Closeable;
import java.io.IOException;

/**
 * Represents a streaming transport response.
 *
 */
@ExperimentalApi
public class FlightTransportResponse<T extends TransportResponse> implements StreamTransportResponse<T>, Closeable {
    private final FlightStream flightStream;
    private final Version version;
    private final NamedWriteableRegistry namedWriteableRegistry;
    private TransportResponseHandler<T> handler;
    private Header currentHeader;
    private VectorSchemaRoot currentRoot;
    private volatile boolean isClosed = false;

    /**
     * It makes a network call to fetch the flight stream, so it should be created async.
     * @param flightClient flight client
     * @param ticket ticket
     * @param version version
     * @param namedWriteableRegistry named writeable registry
     */
    public FlightTransportResponse(
        FlightClient flightClient,
        Ticket ticket,
        Version version,
        NamedWriteableRegistry namedWriteableRegistry
    ) {
        this.version = version;
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.currentHeader = null;
        this.currentRoot = null;
        // its a network call
        this.flightStream = flightClient.getStream(ticket);
        if (flightStream.next()) {
            currentRoot = flightStream.getRoot();
            try {
                currentHeader = parseAndValidateHeader(currentRoot, version);
            } catch (IOException e) {
                throw new TransportException("Failed to parse header", e);
            }
        }
    }

    /**
     * This could be a blocking call depending on whether batch is present on the wire or not;
     * if present, flightStream.next() is lightweight, otherwise, it will wait for the server to produce thereby the
     * thread will be in WAITING state depending on the backpressure strategy used in {@link ArrowFlightProducer}.
     * {@link #setHandler(TransportResponseHandler)}  should be called before calling this method.
     * @return next response in the stream, or null if there are no more responses.
     */
    @Override
    public T nextResponse() {
        if (currentRoot != null) {
            // we lazily deserialize the response only when demanded; header needs to be fetched first,
            // thus are part of constructor; We can revisit this logic if better approach exists on header transmission
            return deserializeResponse();
        } else {
            if (flightStream.next()) {
                currentRoot = flightStream.getRoot();
                return deserializeResponse();
            } else {
                return null;
            }
        }
    }

    /**
     * Set the handler for the response.
     * @param handler handler for the response
     */
    public void setHandler(TransportResponseHandler<T> handler) {
        this.handler = handler;
    }

    /**
     * Returns the header associated with current batch.
     * @return header associated with current batch
     */
    public Header currentHeader() {
        if (currentHeader != null) {
            return currentHeader;
        }
        assert currentRoot != null;
        // this header parsing for subsequent batches aren't needed unless we expect different headers
        // for each batch; We can make it configurable, however, framework will parse it anyway from current batch
        // when requested
        try {
            currentHeader = parseAndValidateHeader(currentRoot, version);
        } catch (IOException e) {
            throw new TransportException("Failed to parse header", e);
        }
        return currentHeader;
    }

    private T deserializeResponse() {
        try {
            if (currentRoot.getRowCount() == 0) {
                throw new IllegalStateException("TransportResponse null");
            }
            try (ArrowStreamInput input = new ArrowStreamInput(currentRoot, namedWriteableRegistry)) {
                return handler.read(input);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize response", e);
        } finally {
            currentRoot.close();
            currentRoot = null;
        }
    }

    private static Header parseAndValidateHeader(VectorSchemaRoot root, Version version) throws IOException {
        VarBinaryVector metaVector = (VarBinaryVector) root.getVector("_meta");
        if (metaVector == null || metaVector.getValueCount() == 0 || metaVector.isNull(0)) {
            throw new TransportException("Missing _meta vector in batch");
        }
        byte[] headerBytes = metaVector.get(0);
        BytesReference headerRef = new BytesArray(headerBytes);
        Header header = InboundDecoder.readHeader(version, headerRef.length(), headerRef);

        if (!Version.CURRENT.isCompatible(header.getVersion())) {
            throw new TransportException("Incompatible version: " + header.getVersion());
        }
        if (TransportStatus.isError(header.getStatus())) {
            throw new TransportException("Received error response");
        }
        return header;
    }

    @Override
    public void close() {
        if (isClosed) {
            return;
        }
        try {
            if (currentRoot != null) {
                currentRoot.close();
            }
            flightStream.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            isClosed = true;
        }
    }
}
