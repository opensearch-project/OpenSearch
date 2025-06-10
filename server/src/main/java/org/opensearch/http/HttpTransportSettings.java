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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.http;

import org.opensearch.common.Booleans;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.transport.PortsRange;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static java.util.Collections.emptyList;
import static org.opensearch.common.settings.Setting.boolSetting;
import static org.opensearch.common.settings.Setting.intSetting;
import static org.opensearch.common.settings.Setting.listSetting;

/**
 * Transport settings for http connections
 *
 * @opensearch.internal
 */
public final class HttpTransportSettings {

    public static final Setting<Boolean> SETTING_CORS_ENABLED = Setting.boolSetting("http.cors.enabled", false, Property.NodeScope);
    public static final Setting<String> SETTING_CORS_ALLOW_ORIGIN = new Setting<>(
        "http.cors.allow-origin",
        "",
        (value) -> value,
        Property.NodeScope
    );
    public static final Setting<Integer> SETTING_CORS_MAX_AGE = intSetting("http.cors.max-age", 1728000, Property.NodeScope);
    public static final Setting<String> SETTING_CORS_ALLOW_METHODS = new Setting<>(
        "http.cors.allow-methods",
        "OPTIONS,HEAD,GET,POST,PUT,DELETE",
        (value) -> value,
        Property.NodeScope
    );
    public static final Setting<String> SETTING_CORS_ALLOW_HEADERS = new Setting<>(
        "http.cors.allow-headers",
        "X-Requested-With,Content-Type,Content-Length",
        (value) -> value,
        Property.NodeScope
    );
    public static final Setting<Boolean> SETTING_CORS_ALLOW_CREDENTIALS = Setting.boolSetting(
        "http.cors.allow-credentials",
        false,
        Property.NodeScope
    );
    public static final Setting<Integer> SETTING_PIPELINING_MAX_EVENTS = intSetting(
        "http.pipelining.max_events",
        10000,
        Property.NodeScope
    );
    public static final Setting<Boolean> SETTING_HTTP_COMPRESSION = Setting.boolSetting("http.compression", true, Property.NodeScope);
    // we intentionally use a different compression level as Netty here as our benchmarks have shown that a compression level of 3 is the
    // best compromise between reduction in network traffic and added latency. For more details please check #7309.
    public static final Setting<Integer> SETTING_HTTP_COMPRESSION_LEVEL = intSetting("http.compression_level", 3, Property.NodeScope);
    public static final Setting<List<String>> SETTING_HTTP_HOST = listSetting(
        "http.host",
        emptyList(),
        Function.identity(),
        Property.NodeScope
    );
    public static final Setting<List<String>> SETTING_HTTP_PUBLISH_HOST = listSetting(
        "http.publish_host",
        SETTING_HTTP_HOST,
        Function.identity(),
        Property.NodeScope
    );
    public static final Setting<List<String>> SETTING_HTTP_BIND_HOST = listSetting(
        "http.bind_host",
        SETTING_HTTP_HOST,
        Function.identity(),
        Property.NodeScope
    );

    public static final Setting<PortsRange> SETTING_HTTP_PORT = new Setting<>(
        "http.port",
        "9200-9300",
        PortsRange::new,
        Property.NodeScope
    );
    public static final Setting<Integer> SETTING_HTTP_PUBLISH_PORT = intSetting("http.publish_port", -1, -1, Property.NodeScope);
    public static final Setting<Boolean> SETTING_HTTP_DETAILED_ERRORS_ENABLED = Setting.boolSetting(
        "http.detailed_errors.enabled",
        true,
        Property.NodeScope
    );
    public static final Setting<Boolean> SETTING_HTTP_CONTENT_TYPE_REQUIRED = new Setting<>(
        "http.content_type.required",
        (s) -> Boolean.toString(true),
        (s) -> {
            final boolean value = Booleans.parseBoolean(s);
            if (value == false) {
                throw new IllegalArgumentException(
                    "http.content_type.required cannot be set to false. It exists only to make a rolling" + " upgrade easier"
                );
            }
            return true;
        },
        Property.NodeScope,
        Property.Deprecated
    );
    public static final Setting<ByteSizeValue> SETTING_HTTP_MAX_CONTENT_LENGTH = Setting.byteSizeSetting(
        "http.max_content_length",
        new ByteSizeValue(100, ByteSizeUnit.MB),
        new ByteSizeValue(0, ByteSizeUnit.BYTES),
        new ByteSizeValue(Integer.MAX_VALUE, ByteSizeUnit.BYTES),
        Property.NodeScope
    );
    public static final Setting<ByteSizeValue> SETTING_HTTP_MAX_CHUNK_SIZE = Setting.byteSizeSetting(
        "http.max_chunk_size",
        new ByteSizeValue(8, ByteSizeUnit.KB),
        Property.NodeScope
    );
    public static final Setting<ByteSizeValue> SETTING_HTTP_MAX_HEADER_SIZE = Setting.byteSizeSetting(
        "http.max_header_size",
        new ByteSizeValue(16, ByteSizeUnit.KB),
        Property.NodeScope
    );
    public static final Setting<Integer> SETTING_HTTP_MAX_WARNING_HEADER_COUNT = intSetting(
        "http.max_warning_header_count",
        -1,
        -1,
        Property.NodeScope
    );
    public static final Setting<ByteSizeValue> SETTING_HTTP_MAX_WARNING_HEADER_SIZE = Setting.byteSizeSetting(
        "http.max_warning_header_size",
        new ByteSizeValue(-1),
        Property.NodeScope
    );
    public static final Setting<ByteSizeValue> SETTING_HTTP_MAX_INITIAL_LINE_LENGTH = Setting.byteSizeSetting(
        "http.max_initial_line_length",
        new ByteSizeValue(4, ByteSizeUnit.KB),
        Property.NodeScope
    );
    // don't reset cookies by default, since I don't think we really need to
    // note, parsing cookies was fixed in netty 3.5.1 regarding stack allocation, but still, currently, we don't need cookies
    public static final Setting<Boolean> SETTING_HTTP_RESET_COOKIES = Setting.boolSetting("http.reset_cookies", false, Property.NodeScope);

    // A default of 0 means that by default there is no read timeout
    public static final Setting<TimeValue> SETTING_HTTP_READ_TIMEOUT = Setting.timeSetting(
        "http.read_timeout",
        new TimeValue(0),
        new TimeValue(0),
        Property.NodeScope
    );

    // A default of 0 means that by default there is no connect timeout
    public static final Setting<TimeValue> SETTING_HTTP_CONNECT_TIMEOUT = Setting.timeSetting(
        "http.connect_timeout",
        new TimeValue(0),
        new TimeValue(0),
        Property.NodeScope
    );

    // Tcp socket settings

    public static final Setting<Boolean> OLD_SETTING_HTTP_TCP_NO_DELAY = boolSetting(
        "http.tcp_no_delay",
        NetworkService.TCP_NO_DELAY,
        Setting.Property.NodeScope,
        Setting.Property.Deprecated
    );
    public static final Setting<Boolean> SETTING_HTTP_TCP_NO_DELAY = boolSetting(
        "http.tcp.no_delay",
        OLD_SETTING_HTTP_TCP_NO_DELAY,
        Setting.Property.NodeScope
    );
    public static final Setting<Boolean> SETTING_HTTP_TCP_KEEP_ALIVE = boolSetting(
        "http.tcp.keep_alive",
        NetworkService.TCP_KEEP_ALIVE,
        Setting.Property.NodeScope
    );
    public static final Setting<Integer> SETTING_HTTP_TCP_KEEP_IDLE = intSetting(
        "http.tcp.keep_idle",
        NetworkService.TCP_KEEP_IDLE,
        -1,
        300,
        Setting.Property.NodeScope
    );
    public static final Setting<Integer> SETTING_HTTP_TCP_KEEP_INTERVAL = intSetting(
        "http.tcp.keep_interval",
        NetworkService.TCP_KEEP_INTERVAL,
        -1,
        300,
        Setting.Property.NodeScope
    );
    public static final Setting<Integer> SETTING_HTTP_TCP_KEEP_COUNT = intSetting(
        "http.tcp.keep_count",
        NetworkService.TCP_KEEP_COUNT,
        -1,
        Setting.Property.NodeScope
    );
    public static final Setting<Boolean> SETTING_HTTP_TCP_REUSE_ADDRESS = boolSetting(
        "http.tcp.reuse_address",
        NetworkService.TCP_REUSE_ADDRESS,
        Setting.Property.NodeScope
    );
    public static final Setting<ByteSizeValue> SETTING_HTTP_TCP_SEND_BUFFER_SIZE = Setting.byteSizeSetting(
        "http.tcp.send_buffer_size",
        NetworkService.TCP_SEND_BUFFER_SIZE,
        Setting.Property.NodeScope
    );
    public static final Setting<ByteSizeValue> SETTING_HTTP_TCP_RECEIVE_BUFFER_SIZE = Setting.byteSizeSetting(
        "http.tcp.receive_buffer_size",
        NetworkService.TCP_RECEIVE_BUFFER_SIZE,
        Setting.Property.NodeScope
    );

    public static final Setting<List<String>> SETTING_HTTP_TRACE_LOG_INCLUDE = Setting.listSetting(
        "http.tracer.include",
        Collections.emptyList(),
        Function.identity(),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    public static final Setting<List<String>> SETTING_HTTP_TRACE_LOG_EXCLUDE = Setting.listSetting(
        "http.tracer.exclude",
        Collections.emptyList(),
        Function.identity(),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private HttpTransportSettings() {}
}
