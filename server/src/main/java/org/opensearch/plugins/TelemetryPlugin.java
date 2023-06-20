/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.common.settings.Settings;
import org.opensearch.telemetry.Telemetry;

import java.util.Optional;

/**
 * Plugin for extending telemetry related classes
 */
public interface TelemetryPlugin {

    Optional<Telemetry> getTelemetry(Settings settings);

    String getName();

}
