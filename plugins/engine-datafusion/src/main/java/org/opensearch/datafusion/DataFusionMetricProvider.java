/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import org.opensearch.datafusion.jni.NativeBridge;
import org.opensearch.vectorized.execution.metrics.DataFusionPluginStats;
import org.opensearch.vectorized.execution.metrics.MetricProvider;
import org.opensearch.vectorized.execution.metrics.PluginStats;

/**
 * DataFusion implementation of {@link MetricProvider} that delegates to
 * {@link NativeBridge} native JNI methods for collecting runtime and task monitor metrics.
 */
public class DataFusionMetricProvider implements MetricProvider {

    @Override
    public PluginStats stats() {
        byte[] bytes = NativeBridge.stats();
        if (bytes == null) {
            return null;
        }
        return DataFusionPluginStats.decode(bytes);
    }
}
