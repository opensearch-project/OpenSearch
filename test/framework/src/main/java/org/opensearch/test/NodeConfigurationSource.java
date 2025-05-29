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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.test;

import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;

public abstract class NodeConfigurationSource {

    public static final NodeConfigurationSource EMPTY = new NodeConfigurationSource() {
        @Override
        public Settings nodeSettings(int nodeOrdinal) {
            return Settings.EMPTY;
        }

        @Override
        public Path nodeConfigPath(int nodeOrdinal) {
            return null;
        }
    };

    /**
     * @return the settings for the node represented by the given ordinal, or {@code null} if there are no settings defined
     */
    public abstract Settings nodeSettings(int nodeOrdinal);

    public abstract Path nodeConfigPath(int nodeOrdinal);

    /** Returns plugins that should be loaded on the node */
    public Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.emptyList();
    }

    /** Returns plugins that should be loaded on the node */
    public Collection<PluginInfo> additionalNodePlugins() {
        return Collections.emptyList();
    }
}
