/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.crypto;

import org.opensearch.OpenSearchException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.rest.RestStatus;

import java.io.IOException;

/**
 * Thrown when crypto manager creation or retrieval fails.
 *
 * @opensearch.internal
 */
public class CryptoRegistryException extends OpenSearchException {
    private final String name;
    private final String type;
    private final RestStatus restStatus;

    /**
     * Constructs a new CryptoRegistryException with the given client name and client type.
     *
     * @param clientName The name of the client for which the crypto registry is missing.
     * @param clientType The type of the client for which the crypto registry is missing.
     */
    public CryptoRegistryException(String clientName, String clientType) {
        super("[Missing crypto registry for client name : " + clientName + " of type " + clientType + " ]");
        this.name = clientName;
        this.type = clientType;
        this.restStatus = RestStatus.NOT_FOUND;
    }

    /**
     * Constructs a new CryptoRegistryException with the given client name, client type, and a cause.
     *
     * @param clientName The name of the client that caused the exception.
     * @param clientType The type of the client that caused the exception.
     * @param cause      The cause of the exception, which could be another throwable.
     */
    public CryptoRegistryException(String clientName, String clientType, Throwable cause) {
        super("[Client name : " + clientName + " Type " + clientType + " ]", cause);
        this.name = clientName;
        this.type = clientType;
        if (cause instanceof IllegalArgumentException) {
            this.restStatus = RestStatus.BAD_REQUEST;
        } else {
            this.restStatus = RestStatus.INTERNAL_SERVER_ERROR;
        }
    }

    /**
     * Constructs a new CryptoRegistryException with the given client name, client type, and a custom message.
     *
     * @param clientName The name of the client that caused the exception.
     * @param clientType The type of the client that caused the exception.
     * @param msg        A custom message to be included in the exception.
     */
    public CryptoRegistryException(String clientName, String clientType, String msg) {
        super("[ " + msg + " Client name : " + clientName + " type " + clientType + " ] ");
        this.name = clientName;
        this.type = clientType;
        this.restStatus = RestStatus.INTERNAL_SERVER_ERROR;
    }

    /**
     * Get the HTTP status associated with this exception.
     *
     * @return The HTTP status code representing the nature of the exception.
     */
    @Override
    public RestStatus status() {
        return restStatus;
    }

    /**
     * Get the name of the client associated with this exception.
     *
     * @return The name of the client for which the exception was raised.
     */
    public String getName() {
        return name;
    }

    /**
     * Get the type of the client associated with this exception.
     *
     * @return The type of the client for which the exception was raised.
     */
    public String getType() {
        return type;
    }

    /**
     * Constructs a new CryptoRegistryException by deserializing it from the provided input stream.
     *
     * @param in The input stream containing the serialized exception data.
     * @throws IOException If an I/O error occurs while reading from the input stream.
     */
    public CryptoRegistryException(StreamInput in) throws IOException {
        super(in);
        this.name = in.readString();
        this.type = in.readString();
        this.restStatus = RestStatus.fromCode(in.readInt());
    }

    /**
     * Write the exception data to the provided output stream for serialization.
     *
     * @param out The output stream to which the exception data should be written.
     * @throws IOException If an I/O error occurs while writing to the output stream.
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(name);
        out.writeString(type);
        out.writeInt(restStatus.getStatus());
    }
}
