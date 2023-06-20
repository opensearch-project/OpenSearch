/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.TelemetryPlugin;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A module for loading classes for telemetry
 *
 * @opensearch.internal
 */
public class TelemetryModule {

    private static final String TELEMETRY_TYPE_DEFAULT = "telemetry.type.default";
    private static final String TELEMETRY_TYPE = "telemetry.type";

    public static final Setting<String> TELEMETRY_DEFAULT_TYPE_SETTING = Setting.simpleString(
        TELEMETRY_TYPE_DEFAULT,
        Setting.Property.NodeScope
    );
    public static final Setting<String> TELEMETRY_TYPE_SETTING = Setting.simpleString(TELEMETRY_TYPE, Setting.Property.NodeScope);

    private final Settings settings;
    private final Map<String, Telemetry> telemetryFactories = new HashMap<>();

    public TelemetryModule(Settings settings, List<TelemetryPlugin> telemetryPlugins, TelemetrySettings telemetrySettings) {
        this.settings = settings;

        for (TelemetryPlugin telemetryPlugin : telemetryPlugins) {
            Optional<Telemetry> telemetry = telemetryPlugin.getTelemetry(telemetrySettings);
            if (telemetry.isPresent()) {
                registerTelemetry(telemetryPlugin.getName(), telemetry.get());
            }
        }
    }

    public Telemetry getTelemetry() {
        final String telemetryType = getTelemetryType();
        return telemetryFactories.get(telemetryType);
    }

    private String getTelemetryType() {
        final String telemetryType = TELEMETRY_TYPE_SETTING.exists(settings)
            ? TELEMETRY_TYPE_SETTING.get(settings)
            : TELEMETRY_DEFAULT_TYPE_SETTING.get(settings);
        return telemetryType;
    }

    private void registerTelemetry(String key, Telemetry factory) {
        if (telemetryFactories.putIfAbsent(key, factory) != null) {
            throw new IllegalArgumentException("telemetry for name: " + key + " is already registered");
        }
    }

}
