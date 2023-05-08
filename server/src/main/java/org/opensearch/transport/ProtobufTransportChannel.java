/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.Version;

import java.io.IOException;

/**
 * A transport channel allows to send a response to a request on the channel.
*
* @opensearch.internal
*/
public interface ProtobufTransportChannel {

    Logger logger = LogManager.getLogger(ProtobufTransportChannel.class);

    String getProfileName();

    String getChannelType();

    void sendResponse(ProtobufTransportResponse response) throws IOException;

    void sendResponse(Exception exception) throws IOException;

    /**
     * Returns the version of the other party that this channel will send a response to.
    */
    default Version getVersion() {
        return Version.CURRENT;
    }

    /**
     * A helper method to send an exception and handle and log a subsequent exception
    */
    static void sendErrorResponse(ProtobufTransportChannel channel, String actionName, ProtobufTransportRequest request, Exception e) {
        try {
            channel.sendResponse(e);
        } catch (Exception sendException) {
            sendException.addSuppressed(e);
            logger.warn(
                () -> new ParameterizedMessage("Failed to send error response for action [{}] and request [{}]", actionName, request),
                sendException
            );
        }
    }
}
