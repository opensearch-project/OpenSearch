/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.nativeprotocol;

import org.opensearch.Version;
import org.opensearch.action.admin.cluster.stats.ClusterStatsAction;
import org.opensearch.action.admin.cluster.stats.ClusterStatsRequest;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.test.junit.annotations.TestLogging;
import org.opensearch.transport.TransportLoggerTests;

import java.io.IOException;

@TestLogging(value = "org.opensearch.transport.TransportLogger:trace", reason = "to ensure we log network events on TRACE level")
public class NativeTransportLoggerTests extends TransportLoggerTests {

    public BytesReference buildRequest() throws IOException {
        boolean compress = randomBoolean();
        try (BytesStreamOutput bytesStreamOutput = new BytesStreamOutput()) {
            NativeOutboundMessage.Request request = new NativeOutboundMessage.Request(
                new ThreadContext(Settings.EMPTY),
                new String[0],
                new ClusterStatsRequest(),
                Version.CURRENT,
                ClusterStatsAction.NAME,
                randomInt(30),
                false,
                compress
            );
            return request.serialize(new BytesStreamOutput());
        }
    }
}
