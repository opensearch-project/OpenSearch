/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.reactor.netty4;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;

import reactor.netty.tcp.TcpServer;

/**
 * The transport implementations based on Reactor Netty (see please {@link TcpServer}).
 */
public class ReactorNetty4Transport {
    /**
     * The number of Netty workers
     */
    public static final Setting<Integer> SETTING_WORKER_COUNT = new Setting<>(
        "transport.netty.worker_count",
        (s) -> Integer.toString(OpenSearchExecutors.allocatedProcessors(s)),
        (s) -> Setting.parseInt(s, 1, "transport.netty.worker_count"),
        Property.NodeScope
    );

    /**
     * Default constructor
     */
    public ReactorNetty4Transport() {}
}
