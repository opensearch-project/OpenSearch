/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.NamedWriteable;
import org.opensearch.core.xcontent.ToXContentFragment;

/**
 * Plugin-contributed payload that surfaces in {@code _nodes/stats} under
 * {@code nodes.<id>.<getWriteableName()>}. Combines the wire-protocol
 * contract ({@link NamedWriteable}) with the JSON-rendering contract
 * ({@link ToXContentFragment}) that {@link org.opensearch.action.admin.cluster.node.stats.NodeStats}
 * needs to merge the contribution into the standard {@code _nodes/stats} response shape.
 *
 * <p>Plugins emit instances of their concrete type from
 * {@link Plugin#nodeStats()} and register the type with
 * {@link Plugin#getNamedWriteables()} so the coordinator can deserialize
 * per-node payloads it received over transport.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface PluginNodeStats extends NamedWriteable, ToXContentFragment {}
