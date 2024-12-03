/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.grpc;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.transport.PortsRange;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;

import java.util.List;
import java.util.function.Function;

import static java.util.Collections.emptyList;
import static org.opensearch.common.settings.Setting.intSetting;
import static org.opensearch.common.settings.Setting.listSetting;

/**
 * Transport settings for gRPC connections
 *
 * @opensearch.internal
 */
public final class GrpcTransportSettings {

    public static final Setting<PortsRange> SETTING_GRPC_PORT = new Setting<>(
        "grpc.port",
        "9400-9500",
        PortsRange::new,
        Property.NodeScope
    );

    public static final Setting<Integer> SETTING_GRPC_PUBLISH_PORT = intSetting("grpc.publish_port", -1, -1, Property.NodeScope);

    public static final Setting<List<String>> SETTING_GRPC_BIND_HOST = listSetting(
        "grpc.bind_host",
        List.of("0.0.0.0"),
        Function.identity(),
        Property.NodeScope
    );

    public static final Setting<List<String>> SETTING_GRPC_HOST = listSetting(
        "grpc.host",
        emptyList(),
        Function.identity(),
        Property.NodeScope
    );

    public static final Setting<List<String>> SETTING_GRPC_PUBLISH_HOST = listSetting(
        "grpc.publish_host",
        SETTING_GRPC_HOST,
        Function.identity(),
        Property.NodeScope
    );

    public static final Setting<ByteSizeValue> SETTING_GRPC_MAX_CONTENT_LENGTH = Setting.byteSizeSetting(
        "grpc.max_content_length",
        new ByteSizeValue(100, ByteSizeUnit.MB),
        new ByteSizeValue(0, ByteSizeUnit.BYTES),
        new ByteSizeValue(Integer.MAX_VALUE, ByteSizeUnit.BYTES),
        Property.NodeScope
    );

    private GrpcTransportSettings() {}
}
