/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.tracing.Telemetry;
import org.opensearch.tracing.TracerSettings;

import java.util.Map;
import java.util.function.Supplier;

/**
 * Plugin for extending tracing related classes
 */
public interface TracerPlugin {

    Map<String, Supplier<Telemetry>> getTelemetries(TracerSettings tracerSettings);

}
