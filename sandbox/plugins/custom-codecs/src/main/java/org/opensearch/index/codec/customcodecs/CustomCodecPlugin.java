/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.customcodecs;

import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.EnginePlugin;

/**
 * A plugin that implements custom codecs. Supports these codecs:
 * <ul>
 * <li>zstd
 * <li>zstdnodict
 * </ul>
 *
 * @opensearch.internal
 */
public final class CustomCodecPlugin extends Plugin implements EnginePlugin {
    /** Creates a new instance. */
    public CustomCodecPlugin() {}
}
