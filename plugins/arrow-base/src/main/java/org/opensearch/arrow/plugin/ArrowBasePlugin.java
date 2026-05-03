/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.plugin;

import org.opensearch.plugins.ExtensiblePlugin;
import org.opensearch.plugins.Plugin;

/**
 * Node-level home for Apache Arrow. Bundles the Arrow libraries and exposes the transport
 * integration contracts under {@code org.opensearch.arrow.transport}. Plugins that produce
 * or consume native Arrow data declare {@code extendedPlugins = ['arrow-base']} so they
 * share this plugin's classloader — a single copy of the Arrow classes lets Arrow-carrying
 * transport types cross plugin boundaries.
 */
public class ArrowBasePlugin extends Plugin implements ExtensiblePlugin {
    /** Default constructor. */
    public ArrowBasePlugin() {}
}
