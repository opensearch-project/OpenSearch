/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.action;

import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.ProtobufWriteable;
import org.opensearch.transport.TransportRequestOptions;

/**
 * A generic action. Should strive to make it a singleton.
*
* @opensearch.api
*/
public class ProtobufActionType<Response extends ProtobufActionResponse> {

    private final String name;
    private final ProtobufWriteable.Reader<Response> responseReader;

    /**
     * @param name The name of the action, must be unique across actions.
    * @param responseReader A reader for the response type
    */
    public ProtobufActionType(String name, ProtobufWriteable.Reader<Response> responseReader) {
        this.name = name;
        this.responseReader = responseReader;
    }

    /**
     * The name of the action. Must be unique across actions.
    */
    public String name() {
        return this.name;
    }

    /**
     * Get a reader that can create a new instance of the class from a {@link byte[]}
    */
    public ProtobufWriteable.Reader<Response> getResponseReaderTry() {
        return responseReader;
    }

    /**
     * Optional request options for the action.
    */
    public TransportRequestOptions transportOptions(Settings settings) {
        return TransportRequestOptions.EMPTY;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof ProtobufActionType && name.equals(((ProtobufActionType<?>) o).name());
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }
}
