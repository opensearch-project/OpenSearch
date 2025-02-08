/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.arrow.spi.StreamManager;

import java.util.function.Supplier;

/**
 * An interface for OpenSearch plugins to implement to provide a StreamManager.
 * Plugins can implement this interface to provide custom StreamManager implementation.
 * @see StreamManager
 */
public interface StreamManagerPlugin {
    /**
     * Returns the StreamManager instance for this plugin.
     *
     * @return The StreamManager instance
     */
    Supplier<StreamManager> getStreamManager();
}
