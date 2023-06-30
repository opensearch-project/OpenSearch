/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.monitor;

import org.opensearch.common.component.AbstractLifecycleComponent;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.monitor.fs.ProtobufFsService;
import org.opensearch.monitor.jvm.JvmGcMonitorService;
import org.opensearch.monitor.jvm.ProtobufJvmService;
import org.opensearch.monitor.os.ProtobufOsService;
import org.opensearch.monitor.process.ProtobufProcessService;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;

/**
 * The resource monitoring service
 *
 * @opensearch.internal
 */
public class ProtobufMonitorService extends AbstractLifecycleComponent {

    private final JvmGcMonitorService jvmGcMonitorService;
    private final ProtobufOsService osService;
    private final ProtobufProcessService processService;
    private final ProtobufJvmService jvmService;
    private final ProtobufFsService fsService;

    public ProtobufMonitorService(Settings settings, NodeEnvironment nodeEnvironment, ThreadPool threadPool, FileCache fileCache)
        throws IOException {
        this.jvmGcMonitorService = new JvmGcMonitorService(settings, threadPool);
        this.osService = new ProtobufOsService(settings);
        this.processService = new ProtobufProcessService(settings);
        this.jvmService = new ProtobufJvmService(settings);
        this.fsService = new ProtobufFsService(settings, nodeEnvironment, fileCache);
    }

    public ProtobufOsService osService() {
        return this.osService;
    }

    public ProtobufProcessService processService() {
        return this.processService;
    }

    public ProtobufJvmService jvmService() {
        return this.jvmService;
    }

    public ProtobufFsService fsService() {
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
