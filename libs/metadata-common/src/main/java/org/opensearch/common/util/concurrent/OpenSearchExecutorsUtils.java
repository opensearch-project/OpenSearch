/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util.concurrent;

import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;

import java.util.function.Function;

/**
 * Executors Utilities.
 *
 * @opensearch.internal
 */
public class OpenSearchExecutorsUtils {

    private OpenSearchExecutorsUtils() {}

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(OpenSearchExecutorsUtils.class);

    /**
     * Setting to manually set the number of available processors. This setting is used to adjust thread pool sizes per node.
     */
    public static final Setting<Integer> PROCESSORS_SETTING = new Setting<>(
        "processors",
        s -> Integer.toString(Runtime.getRuntime().availableProcessors()),
        processorsParser("processors"),
        Setting.Property.Deprecated,
        Setting.Property.NodeScope
    );

    /**
     * Setting to manually control the number of allocated processors. This setting is used to adjust thread pool sizes per node. The
     * default value is {@link Runtime#availableProcessors()} but should be manually controlled if not all processors on the machine are
     * available to OpenSearch (e.g., because of CPU limits).
     */
    public static final Setting<Integer> NODE_PROCESSORS_SETTING = new Setting<>(
        "node.processors",
        PROCESSORS_SETTING,
        processorsParser("node.processors"),
        Setting.Property.NodeScope
    );

    private static Function<String, Integer> processorsParser(final String name) {
        return s -> {
            final int value = Setting.parseInt(s, 1, name);
            final int availableProcessors = Runtime.getRuntime().availableProcessors();
            if (value > availableProcessors) {
                deprecationLogger.deprecate(
                    "processors_" + name,
                    "setting [{}] to value [{}] which is more than available processors [{}] is deprecated",
                    name,
                    value,
                    availableProcessors
                );
            }
            return value;
        };
    }

    /**
     * Returns the number of allocated processors. Defaults to {@link Runtime#availableProcessors()} but can be overridden by passing a
     * {@link Settings} instance with the key {@code node.processors} set to the desired value.
     *
     * @param settings a {@link Settings} instance from which to derive the allocated processors
     * @return the number of allocated processors
     */
    public static int allocatedProcessors(final Settings settings) {
        return NODE_PROCESSORS_SETTING.get(settings);
    }
}
