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

package org.opensearch.monitor;

import org.opensearch.common.component.AbstractLifecycleComponent;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.monitor.fs.FsService;
import org.opensearch.monitor.jvm.JvmGcMonitorService;
import org.opensearch.monitor.jvm.JvmService;
import org.opensearch.monitor.os.OsService;
import org.opensearch.monitor.process.ProcessService;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;

/**
 * The resource monitoring service
 *
 * @opensearch.internal
 */
public class MonitorService extends AbstractLifecycleComponent {

    private final JvmGcMonitorService jvmGcMonitorService;
    private final OsService osService;
    private final ProcessService processService;
    private final JvmService jvmService;
    private final FsService fsService;

    public MonitorService(Settings settings, NodeEnvironment nodeEnvironment, ThreadPool threadPool) throws IOException {
        this.jvmGcMonitorService = new JvmGcMonitorService(settings, threadPool);
        this.osService = new OsService(settings);
        this.processService = new ProcessService(settings);
        this.jvmService = new JvmService(settings);
        this.fsService = new FsService(settings, nodeEnvironment);
    }

    public OsService osService() {
        return this.osService;
    }

    public ProcessService processService() {
        return this.processService;
    }

    public JvmService jvmService() {
        return this.jvmService;
    }

    public FsService fsService() {
        return this.fsService;
    }

    @Override
    protected void doStart() {
        jvmGcMonitorService.start();
    }

    @Override
    protected void doStop() {
        jvmGcMonitorService.stop();
    }

    @Override
    protected void doClose() {
        jvmGcMonitorService.close();
    }

}
