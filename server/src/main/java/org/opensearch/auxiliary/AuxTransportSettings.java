/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.auxiliary;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.transport.PortsRange;

import java.util.List;
import java.util.function.Function;

import static java.util.Collections.emptyList;
import static org.opensearch.common.settings.Setting.intSetting;
import static org.opensearch.common.settings.Setting.listSetting;

/**
 * Auxiliary transport server settings.
 *
 * @opensearch.internal
 */
public final class AuxTransportSettings {

    public static final Setting<PortsRange> SETTING_AUX_PORT = new Setting<>("aux.port", "9400-9500", PortsRange::new, Property.NodeScope);

    public static final Setting<Integer> SETTING_AUX_PUBLISH_PORT = intSetting("aux.publish_port", -1, -1, Setting.Property.NodeScope);

    public static final Setting<List<String>> SETTING_AUX_HOST = listSetting(
        "aux.host",
        emptyList(),
        Function.identity(),
        Setting.Property.NodeScope
    );

    public static final Setting<List<String>> SETTING_AUX_PUBLISH_HOST = listSetting(
        "aux.publish_host",
        SETTING_AUX_HOST,
        Function.identity(),
        Setting.Property.NodeScope
    );

    public static final Setting<List<String>> SETTING_AUX_BIND_HOST = listSetting(
        "aux.bind_host",
        SETTING_AUX_HOST,
        Function.identity(),
        Setting.Property.NodeScope
    );

    private AuxTransportSettings() {}
}
