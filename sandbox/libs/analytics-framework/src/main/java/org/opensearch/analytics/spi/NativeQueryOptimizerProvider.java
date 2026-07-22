/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.opensearch.common.settings.Settings;

public interface NativeQueryOptimizerProvider {

    String name();

    long createNativeOptimizer(Settings settings);
}
