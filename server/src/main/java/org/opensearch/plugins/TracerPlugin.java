/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.telemetry.Telemetry;
import org.opensearch.telemetry.tracing.TracerSettings;

import java.util.Map;
import java.util.function.Supplier;

/**
 * Plugin for extending telemetry related classes
 */
public interface TracerPlugin {

    Map<String, Supplier<Telemetry>> getTelemetries(TracerSettings tracerSettings);

}
