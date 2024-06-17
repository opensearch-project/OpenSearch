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

package org.opensearch.plugins;

import org.opensearch.common.settings.Settings;
import org.opensearch.indices.SystemIndexDescriptor;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Plugin for defining system indices. Extends {@link ActionPlugin} because system indices must be accessed via APIs
 * added by the plugin that owns the system index, rather than standard APIs.
 *
 * @opensearch.api
 */
public interface SystemIndexPlugin extends ActionPlugin {

    /**
     * Returns a {@link Collection} of {@link SystemIndexDescriptor}s that describe this plugin's system indices, including
     * name, mapping, and settings.
     * @param settings The node's settings
     * @return Descriptions of the system indices managed by this plugin.
     */
    default Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
        return Collections.emptyList();
    }

    /**
     * This function passes the registered system indices to a plugin.
     *
     * Note: Only one installed plugin may implement onSystemIndices.
     */
    default Consumer<Map<String, Set<String>>> onSystemIndices() {
        return null;
    }
}
