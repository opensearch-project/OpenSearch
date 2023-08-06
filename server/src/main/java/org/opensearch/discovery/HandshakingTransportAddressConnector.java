/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.discovery;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.Version;
import org.opensearch.action.ActionListener;
import org.opensearch.action.NotifyOnceListener;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.Randomness;
import org.opensearch.common.UUIDs;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.discovery.PeerFinder.TransportAddressConnector;
import org.opensearch.transport.ConnectTransportException;
import org.opensearch.transport.ConnectionProfile;
import org.opensearch.transport.Transport.Connection;
import org.opensearch.transport.TransportRequestOptions.Type;
import org.opensearch.transport.TransportService;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;

/**
 * Connector for transport handshake
 *
 * @opensearch.internal
 */
public class HandshakingTransportAddressConnector implements TransportAddressConnector {

    private static final Logger logger = LogManager.getLogger(HandshakingTransportAddressConnector.class);

    // connection timeout for probes
    public static final Setting<TimeValue> PROBE_CONNECT_TIMEOUT_SETTING = Setting.timeSetting(
        "discovery.probe.connect_timeout",
        TimeValue.timeValueMillis(3000),
        TimeValue.timeValueMillis(1),
        Setting.Property.NodeScope
    );
    // handshake timeout for probes
    public static final Setting<TimeValue> PROBE_HANDSHAKE_TIMEOUT_SETTING = Setting.timeSetting(
        "discovery.probe.handshake_timeout",
        TimeValue.timeValueMillis(1000),
        TimeValue.timeValueMillis(1),
        Setting.Property.NodeScope
    );

    private final TransportService transportService;
    private final TimeValue probeConnectTimeout;
    private final TimeValue probeHandshakeTimeout;

    public HandshakingTransportAddressConnector(Settings settings, TransportService transportService) {
        this.transportService = transportService;
        probeConnectTimeout = PROBE_CONNECT_TIMEOUT_SETTING.get(settings);
        probeHandshakeTimeout = PROBE_HANDSHAKE_TIMEOUT_SETTING.get(settings);
    }

    @Override
    public void connectToRemoteMasterNode(TransportAddress transportAddress, ActionListener<DiscoveryNode> listener) {
        transportService.getThreadPool().generic().execute(new AbstractRunnable() {
            private final AbstractRunnable thisConnectionAttempt = this;

            @Override
            protected void doRun() {
                // We could skip this if the transportService were already connected to the given address, but the savings would be minimal
                // so we open a new connection anyway.

                final DiscoveryNode targetNode = new DiscoveryNode(
                    "",
                    transportAddress.toString(),
                    UUIDs.randomBase64UUID(Randomness.get()), // generated deterministically for reproducible tests
                    transportAddress.address().getHostString(),
                    transportAddress.getAddress(),
                    transportAddress,
                    emptyMap(),
                    emptySet(),
                    Version.CURRENT.minimumCompatibilityVersion()
                );

                logger.trace("[{}] opening probe connection", thisConnectionAttempt);
                transportService.openConnection(
                    targetNode,
                    ConnectionProfile.buildSingleChannelProfile(
                        Type.REG,
                        probeConnectTimeout,
                        probeHandshakeTimeout,
                        TimeValue.MINUS_ONE,
                        null
                    ),
                    new ActionListener<Connection>() {
                        @Override
                        public void onResponse(Connection connection) {
                            logger.trace("[{}] opened probe connection", thisConnectionAttempt);

                            // use NotifyOnceListener to make sure the following line does not result in onFailure being called when
                            // the connection is closed in the onResponse handler
                            transportService.handshake(connection, probeHandshakeTimeout.millis(), new NotifyOnceListener<DiscoveryNode>() {

                                @Override
                                protected void innerOnResponse(DiscoveryNode remoteNode) {
                                    try {
                                        // success means (amongst other things) that the cluster names match
                                        logger.trace("[{}] handshake successful: {}", thisConnectionAttempt, remoteNode);
                                        IOUtils.closeWhileHandlingException(connection);

                                        if (remoteNode.equals(transportService.getLocalNode())) {
                                            listener.onFailure(new ConnectTransportException(remoteNode, "local node found"));
                                        } else if (remoteNode.isClusterManagerNode() == false) {
                                            listener.onFailure(
                                                new ConnectTransportException(remoteNode, "non-cluster-manager-eligible node found")
                                            );
                                        } else {
                                            transportService.connectToNode(remoteNode, new ActionListener<Void>() {
                                                @Override
                                                public void onResponse(Void ignored) {
                                                    logger.trace(
                                                        "[{}] completed full connection with [{}]",
                                                        thisConnectionAttempt,
                                                        remoteNode
                                                    );
                                                    listener.onResponse(remoteNode);
                                                }

                                                @Override
                                                public void onFailure(Exception e) {
                                                    // we opened a connection and successfully performed a handshake, so we're definitely
                                                    // talking to a cluster-manager-eligible node with a matching cluster name and a good
                                                    // version,
                                                    // but the attempt to open a full connection to its publish address failed; a common
                                                    // reason is that the remote node is listening on 0.0.0.0 but has made an inappropriate
                                                    // choice for its publish address.
                                                    logger.warn(
                                                        new ParameterizedMessage(
                                                            "[{}] completed handshake with [{}] but followup connection failed",
                                                            thisConnectionAttempt,
                                                            remoteNode
                                                        ),
                                                        e
                                                    );
                                                    listener.onFailure(e);
                                                }
                                            });
                                        }
                                    } catch (Exception e) {
                                        listener.onFailure(e);
                                    }
                                }

                                @Override
                                protected void innerOnFailure(Exception e) {
                                    // we opened a connection and successfully performed a low-level handshake, so we were definitely
                                    // talking to an Elasticsearch node, but the high-level handshake failed indicating some kind of
                                    // mismatched configurations (e.g. cluster name) that the user should address
                                    logger.warn(new ParameterizedMessage("handshake failed for [{}]", thisConnectionAttempt), e);
                                    IOUtils.closeWhileHandlingException(connection);
                                    listener.onFailure(e);
                                }

                            });

                        }

                        @Override
                        public void onFailure(Exception e) {
                            listener.onFailure(e);
                        }
                    }
                );
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }

            @Override
            public String toString() {
                return "connectToRemoteMasterNode[" + transportAddress + "]";
            }
        });
    }
}
