/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.transport.PortsRange;
import org.opensearch.core.common.transport.BoundTransportAddress;

import java.util.List;
import java.util.function.Function;

import static java.util.Collections.emptyList;
import static org.opensearch.common.settings.Setting.affixKeySetting;

/**
 * Auxiliary transports are lifecycle components with an associated port range.
 * These pluggable client/server transport implementations have their lifecycle managed by Node.
 *
 * Auxiliary transports are additionally defined by a port range on which they bind. Opening permissions on these
 * ports is awkward as {@link org.opensearch.bootstrap.Security} is configured previous to Node initialization during
 * bootstrap. To allow pluggable AuxTransports access to configurable port ranges we require the port range be provided
 * through an {@link org.opensearch.common.settings.Setting.AffixSetting} of the form 'AUX_SETTINGS_PREFIX.{aux-transport-key}.ports'.
 */
@ExperimentalApi
public abstract class AuxTransport extends AbstractLifecycleComponent {
    public static final String AUX_SETTINGS_PREFIX = "aux.transport.";
    public static final String AUX_TRANSPORT_TYPES_KEY = AUX_SETTINGS_PREFIX + "types";
    public static final String AUX_PORT_DEFAULTS = "9400-9500";
    public static final Setting.AffixSetting<PortsRange> AUX_TRANSPORT_PORT = affixKeySetting(
        AUX_SETTINGS_PREFIX,
        "port",
        key -> new Setting<>(key, AUX_PORT_DEFAULTS, PortsRange::new, Setting.Property.NodeScope)
    );
    public static final Setting<List<String>> AUX_TRANSPORT_TYPES_SETTING = Setting.listSetting(
        AUX_TRANSPORT_TYPES_KEY,
        emptyList(),
        Function.identity(),
        Setting.Property.NodeScope
    );

    /**
     * @return unique name for which this transport is identified in settings.
     * This should identify your transport for the purposes of enabling with AUX_TRANSPORT_TYPES_KEY setting.
     */
    public abstract String settingKey();

    // public for tests
    public abstract BoundTransportAddress getBoundAddress();
}
