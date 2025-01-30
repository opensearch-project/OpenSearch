/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.impl;

import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.ExceptionsHelper;
import org.opensearch.arrow.spi.StreamReader;

/**
 * FlightStreamReader is a wrapper class that adapts the FlightStream interface
 * to the StreamReader interface.
 */
public class FlightStreamReader implements StreamReader {

    private final FlightStream flightStream;

    /**
     * Constructs a FlightStreamReader with the given FlightStream.
     *
     * @param flightStream The FlightStream to be adapted.
     */
    public FlightStreamReader(FlightStream flightStream) {
        this.flightStream = flightStream;
    }

    /**
     * Moves the flightStream to the next batch of data.
     * @return true if there is a next batch of data, false otherwise.
     * @throws FlightRuntimeException if an error occurs while advancing to the next batch like early termination of stream
     */
    @Override
    public boolean next() throws FlightRuntimeException {
        return flightStream.next();
    }

    /**
     * Returns the VectorSchemaRoot containing the current batch of data.
     * @return The VectorSchemaRoot containing the current batch of data.
     * @throws FlightRuntimeException if an error occurs while retrieving the root like early termination of stream
     */
    @Override
    public VectorSchemaRoot getRoot() throws FlightRuntimeException {
        return flightStream.getRoot();
    }

    /**
     * Closes the flightStream.
     */
    @Override
    public void close() {
        ExceptionsHelper.catchAsRuntimeException(flightStream::close);
    }
}
