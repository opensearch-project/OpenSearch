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

package org.opensearch;

import org.opensearch.common.network.NetworkModule;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.nio.NioTransportPlugin;

import java.util.Collection;
import java.util.Collections;

public abstract class NioIntegTestCase extends OpenSearchIntegTestCase {

    @Override
    protected boolean ignoreExternalCluster() {
        return true;
    }

    @Override
    protected boolean addMockTransportService() {
        return false;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal));
        // randomize nio settings
        if (randomBoolean()) {
            builder.put(NioTransportPlugin.NIO_WORKER_COUNT.getKey(), random().nextInt(3) + 1);
            builder.put(NioTransportPlugin.NIO_HTTP_WORKER_COUNT.getKey(), random().nextInt(3) + 1);
        }
        builder.put(NetworkModule.TRANSPORT_TYPE_KEY, NioTransportPlugin.NIO_TRANSPORT_NAME);
        builder.put(NetworkModule.HTTP_TYPE_KEY, NioTransportPlugin.NIO_HTTP_TRANSPORT_NAME);
        return builder.build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(NioTransportPlugin.class);
    }

}
