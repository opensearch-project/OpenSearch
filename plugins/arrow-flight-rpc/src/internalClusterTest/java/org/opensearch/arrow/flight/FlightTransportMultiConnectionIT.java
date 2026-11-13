/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight;

import org.opensearch.common.settings.Settings;
import org.opensearch.transport.StreamTransportService;

/**
 * Runs the {@link FlightTransportIT} scenarios with several stream transport connections per node, so streams
 * between nodes are spread over more than one Flight connection.
 */
public class FlightTransportMultiConnectionIT extends FlightTransportIT {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(StreamTransportService.STREAM_TRANSPORT_CONNECTIONS_PER_NODE_SETTING.getKey(), randomIntBetween(2, 4))
            .build();
    }
}
