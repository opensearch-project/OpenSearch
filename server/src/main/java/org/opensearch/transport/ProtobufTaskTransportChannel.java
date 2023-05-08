/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.transport;

import org.opensearch.Version;
import org.opensearch.common.lease.Releasable;

import java.io.IOException;

/**
 * Transport channel for tasks
*
* @opensearch.internal
*/
public class ProtobufTaskTransportChannel implements ProtobufTransportChannel {

    private final ProtobufTransportChannel channel;
    private final Releasable onTaskFinished;

    ProtobufTaskTransportChannel(ProtobufTransportChannel channel, Releasable onTaskFinished) {
        this.channel = channel;
        this.onTaskFinished = onTaskFinished;
    }

    @Override
    public String getProfileName() {
        return channel.getProfileName();
    }

    @Override
    public String getChannelType() {
        return channel.getChannelType();
    }

    @Override
    public void sendResponse(ProtobufTransportResponse response) throws IOException {
        try {
            onTaskFinished.close();
        } finally {
            channel.sendResponse(response);
        }
    }

    @Override
    public void sendResponse(Exception exception) throws IOException {
        try {
            onTaskFinished.close();
        } finally {
            channel.sendResponse(exception);
        }
    }

    @Override
    public Version getVersion() {
        return channel.getVersion();
    }

    public ProtobufTransportChannel getChannel() {
        return channel;
    }
}
