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

package org.opensearch.repositories.s3;

import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.common.util.CollectionUtils;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.ReloadablePlugin;
import org.opensearch.plugins.RepositoryPlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.s3.async.AsyncExecutorContainer;
import org.opensearch.repositories.s3.async.AsyncTransferEventLoopGroup;
import org.opensearch.repositories.s3.async.AsyncTransferManager;
import org.opensearch.repositories.s3.async.SizeBasedBlockingQ;
import org.opensearch.repositories.s3.async.TransferSemaphoresHolder;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.ScalingExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.watcher.ResourceWatcherService;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * A plugin to add a repository type that writes to and from the AWS S3.
 */
public class S3RepositoryPlugin extends Plugin implements RepositoryPlugin, ReloadablePlugin {

    private static final String URGENT_FUTURE_COMPLETION = "urgent_future_completion";
    private static final String URGENT_STREAM_READER = "urgent_stream_reader";
    private static final String PRIORITY_FUTURE_COMPLETION = "priority_future_completion";
    private static final String PRIORITY_STREAM_READER = "priority_stream_reader";
    private static final String FUTURE_COMPLETION = "future_completion";
    private static final String STREAM_READER = "stream_reader";
    private static final String LOW_TRANSFER_QUEUE_CONSUMER = "low_transfer_queue_consumer";
    private static final String NORMAL_TRANSFER_QUEUE_CONSUMER = "normal_transfer_queue_consumer";

    protected final S3Service service;
    private final S3AsyncService s3AsyncService;

    private final Path configPath;

    private AsyncExecutorContainer urgentExecutorBuilder;
    private AsyncExecutorContainer priorityExecutorBuilder;
    private AsyncExecutorContainer normalExecutorBuilder;
    private ExecutorService lowTransferQConsumerService;
    private ExecutorService normalTransferQConsumerService;
    private SizeBasedBlockingQ normalPrioritySizeBasedBlockingQ;
    private SizeBasedBlockingQ lowPrioritySizeBasedBlockingQ;
    private TransferSemaphoresHolder transferSemaphoresHolder;
    private GenericStatsMetricPublisher genericStatsMetricPublisher;

    public S3RepositoryPlugin(final Settings settings, final Path configPath) {
        this(settings, configPath, new S3Service(configPath), new S3AsyncService(configPath));
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        List<ExecutorBuilder<?>> executorBuilders = new ArrayList<>();
        int halfProc = halfNumberOfProcessors(allocatedProcessors(settings));
        executorBuilders.add(
            new FixedExecutorBuilder(settings, URGENT_FUTURE_COMPLETION, urgentPoolCount(settings), 10_000, URGENT_FUTURE_COMPLETION)
        );
        executorBuilders.add(new ScalingExecutorBuilder(URGENT_STREAM_READER, 1, halfProc, TimeValue.timeValueMinutes(5)));
        executorBuilders.add(
            new ScalingExecutorBuilder(PRIORITY_FUTURE_COMPLETION, 1, allocatedProcessors(settings), TimeValue.timeValueMinutes(5))
        );
        executorBuilders.add(new ScalingExecutorBuilder(PRIORITY_STREAM_READER, 1, halfProc, TimeValue.timeValueMinutes(5)));

        executorBuilders.add(
            new ScalingExecutorBuilder(FUTURE_COMPLETION, 1, allocatedProcessors(settings), TimeValue.timeValueMinutes(5))
        );
        executorBuilders.add(
            new ScalingExecutorBuilder(
                STREAM_READER,
                allocatedProcessors(settings),
                4 * allocatedProcessors(settings),
                TimeValue.timeValueMinutes(5)
            )
        );
        executorBuilders.add(
            new FixedExecutorBuilder(
                settings,
                LOW_TRANSFER_QUEUE_CONSUMER,
                lowPriorityTransferQConsumers(settings),
                10,
                "thread_pool." + LOW_TRANSFER_QUEUE_CONSUMER
            )
        );
        executorBuilders.add(
            new FixedExecutorBuilder(
                settings,
                NORMAL_TRANSFER_QUEUE_CONSUMER,
                normalPriorityTransferQConsumers(settings),
                10,
                "thread_pool." + NORMAL_TRANSFER_QUEUE_CONSUMER
            )
        );
        return executorBuilders;
    }

    private int lowPriorityTransferQConsumers(Settings settings) {
        double lowPriorityAllocation = ((double) (100 - S3Repository.S3_PRIORITY_PERMIT_ALLOCATION_PERCENT.get(settings))) / 100;
        return Math.max(2, (int) (lowPriorityAllocation * S3Repository.S3_TRANSFER_QUEUE_CONSUMERS.get(settings)));
    }

    private int normalPriorityTransferQConsumers(Settings settings) {
        return S3Repository.S3_TRANSFER_QUEUE_CONSUMERS.get(settings);
    }

    static int halfNumberOfProcessors(int numberOfProcessors) {
        return (numberOfProcessors + 1) / 2;
    }

    S3RepositoryPlugin(final Settings settings, final Path configPath, final S3Service service, final S3AsyncService s3AsyncService) {
        this.service = Objects.requireNonNull(service, "S3 service must not be null");
        this.configPath = configPath;
        // eagerly load client settings so that secure settings are read
        Map<String, S3ClientSettings> clientsSettings = S3ClientSettings.load(settings, configPath);
        this.s3AsyncService = Objects.requireNonNull(s3AsyncService, "S3AsyncService must not be null");
        this.service.refreshAndClearCache(clientsSettings);
        this.s3AsyncService.refreshAndClearCache(clientsSettings);
    }

    private static int boundedBy(int value, int min, int max) {
        return Math.min(max, Math.max(min, value));
    }

    private static int allocatedProcessors(Settings settings) {
        return OpenSearchExecutors.allocatedProcessors(settings);
    }

    private static int urgentPoolCount(Settings settings) {
        return boundedBy((allocatedProcessors(settings) + 1) / 2, 1, 2);
    }

    private static int priorityPoolCount(Settings settings) {
        return boundedBy((allocatedProcessors(settings) + 1) / 2, 2, 4);
    }

    private static int normalPoolCount(Settings settings) {
        return boundedBy((allocatedProcessors(settings) + 7) / 8, 1, 2);
    }

    @Override
    public Collection<Object> createComponents(
        final Client client,
        final ClusterService clusterService,
        final ThreadPool threadPool,
        final ResourceWatcherService resourceWatcherService,
        final ScriptService scriptService,
        final NamedXContentRegistry xContentRegistry,
        final Environment environment,
        final NodeEnvironment nodeEnvironment,
        final NamedWriteableRegistry namedWriteableRegistry,
        final IndexNameExpressionResolver expressionResolver,
        final Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        int urgentEventLoopThreads = urgentPoolCount(clusterService.getSettings());
        int priorityEventLoopThreads = priorityPoolCount(clusterService.getSettings());
        int normalEventLoopThreads = normalPoolCount(clusterService.getSettings());
        this.urgentExecutorBuilder = new AsyncExecutorContainer(
            threadPool.executor(URGENT_FUTURE_COMPLETION),
            threadPool.executor(URGENT_STREAM_READER),
            new AsyncTransferEventLoopGroup(urgentEventLoopThreads)
        );
        this.priorityExecutorBuilder = new AsyncExecutorContainer(
            threadPool.executor(PRIORITY_FUTURE_COMPLETION),
            threadPool.executor(PRIORITY_STREAM_READER),
            new AsyncTransferEventLoopGroup(priorityEventLoopThreads)
        );
        this.normalExecutorBuilder = new AsyncExecutorContainer(
            threadPool.executor(FUTURE_COMPLETION),
            threadPool.executor(STREAM_READER),
            new AsyncTransferEventLoopGroup(normalEventLoopThreads)
        );

        this.lowTransferQConsumerService = threadPool.executor(LOW_TRANSFER_QUEUE_CONSUMER);
        this.normalTransferQConsumerService = threadPool.executor(NORMAL_TRANSFER_QUEUE_CONSUMER);

        // High number of permit allocation because each op acquiring permit performs disk IO, computation and network IO.
        int availablePermits = Math.max(allocatedProcessors(clusterService.getSettings()) * 4, 10);
        double priorityPermitAllocation = ((double) S3Repository.S3_PRIORITY_PERMIT_ALLOCATION_PERCENT.get(clusterService.getSettings()))
            / 100;
        int normalPriorityPermits = (int) (priorityPermitAllocation * availablePermits);
        int lowPriorityPermits = availablePermits - normalPriorityPermits;

        int normalPriorityConsumers = normalPriorityTransferQConsumers(clusterService.getSettings());
        int lowPriorityConsumers = lowPriorityTransferQConsumers(clusterService.getSettings());

        ByteSizeValue normalPriorityQCapacity = new ByteSizeValue(normalPriorityConsumers * 10L, ByteSizeUnit.GB);
        ByteSizeValue lowPriorityQCapacity = new ByteSizeValue(lowPriorityConsumers * 20L, ByteSizeUnit.GB);

        this.genericStatsMetricPublisher = new GenericStatsMetricPublisher(
            normalPriorityQCapacity.getBytes(),
            normalPriorityPermits,
            lowPriorityQCapacity.getBytes(),
            lowPriorityPermits
        );

        this.normalPrioritySizeBasedBlockingQ = new SizeBasedBlockingQ(
            normalPriorityQCapacity,
            normalTransferQConsumerService,
            normalPriorityConsumers,
            genericStatsMetricPublisher,
            SizeBasedBlockingQ.QueueEventType.NORMAL
        );

        LowPrioritySizeBasedBlockingQ lowPrioritySizeBasedBlockingQ = new LowPrioritySizeBasedBlockingQ(
            lowPriorityQCapacity,
            lowTransferQConsumerService,
            lowPriorityConsumers,
            genericStatsMetricPublisher
        );
        this.lowPrioritySizeBasedBlockingQ = lowPrioritySizeBasedBlockingQ;
        this.transferSemaphoresHolder = new TransferSemaphoresHolder(
            normalPriorityPermits,
            lowPriorityPermits,
            S3Repository.S3_PERMIT_WAIT_DURATION_MIN.get(clusterService.getSettings()),
            TimeUnit.MINUTES,
            genericStatsMetricPublisher
        );

        return CollectionUtils.arrayAsArrayList(this.normalPrioritySizeBasedBlockingQ, lowPrioritySizeBasedBlockingQ);
    }

    // New class because in core, components are injected via guice only by instance creation due to which
    // same binding types fail.
    private static final class LowPrioritySizeBasedBlockingQ extends SizeBasedBlockingQ {
        public LowPrioritySizeBasedBlockingQ(
            ByteSizeValue capacity,
            ExecutorService executorService,
            int consumers,
            GenericStatsMetricPublisher genericStatsMetricPublisher
        ) {
            super(capacity, executorService, consumers, genericStatsMetricPublisher, QueueEventType.LOW);
        }
    }

    // proxy method for testing
    protected S3Repository createRepository(
        final RepositoryMetadata metadata,
        final NamedXContentRegistry registry,
        final ClusterService clusterService,
        final RecoverySettings recoverySettings
    ) {

        AsyncTransferManager asyncUploadUtils = new AsyncTransferManager(
            S3Repository.PARALLEL_MULTIPART_UPLOAD_MINIMUM_PART_SIZE_SETTING.get(clusterService.getSettings()).getBytes(),
            normalExecutorBuilder.getStreamReader(),
            priorityExecutorBuilder.getStreamReader(),
            urgentExecutorBuilder.getStreamReader(),
            transferSemaphoresHolder
        );
        return new S3Repository(
            metadata,
            registry,
            service,
            clusterService,
            recoverySettings,
            asyncUploadUtils,
            urgentExecutorBuilder,
            priorityExecutorBuilder,
            normalExecutorBuilder,
            s3AsyncService,
            S3Repository.PARALLEL_MULTIPART_UPLOAD_ENABLED_SETTING.get(clusterService.getSettings()),
            configPath,
            normalPrioritySizeBasedBlockingQ,
            lowPrioritySizeBasedBlockingQ,
            genericStatsMetricPublisher
        );
    }

    @Override
    public Map<String, Repository.Factory> getRepositories(
        final Environment env,
        final NamedXContentRegistry registry,
        final ClusterService clusterService,
        final RecoverySettings recoverySettings
    ) {
        return Collections.singletonMap(
            S3Repository.TYPE,
            metadata -> createRepository(metadata, registry, clusterService, recoverySettings)
        );
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(
            // named s3 client configuration settings
            S3ClientSettings.ACCESS_KEY_SETTING,
            S3ClientSettings.SECRET_KEY_SETTING,
            S3ClientSettings.SESSION_TOKEN_SETTING,
            S3ClientSettings.ENDPOINT_SETTING,
            S3ClientSettings.PROTOCOL_SETTING,
            S3ClientSettings.PROXY_TYPE_SETTING,
            S3ClientSettings.PROXY_HOST_SETTING,
            S3ClientSettings.PROXY_PORT_SETTING,
            S3ClientSettings.PROXY_USERNAME_SETTING,
            S3ClientSettings.PROXY_PASSWORD_SETTING,
            S3ClientSettings.READ_TIMEOUT_SETTING,
            S3ClientSettings.MAX_RETRIES_SETTING,
            S3ClientSettings.USE_THROTTLE_RETRIES_SETTING,
            S3ClientSettings.USE_PATH_STYLE_ACCESS,
            S3Repository.ACCESS_KEY_SETTING,
            S3Repository.SECRET_KEY_SETTING,
            S3ClientSettings.SIGNER_OVERRIDE,
            S3ClientSettings.REGION,
            S3ClientSettings.ROLE_ARN_SETTING,
            S3ClientSettings.IDENTITY_TOKEN_FILE_SETTING,
            S3ClientSettings.ROLE_SESSION_NAME_SETTING,
            S3Repository.PARALLEL_MULTIPART_UPLOAD_MINIMUM_PART_SIZE_SETTING,
            S3Repository.PARALLEL_MULTIPART_UPLOAD_ENABLED_SETTING,
            S3Repository.REDIRECT_LARGE_S3_UPLOAD,
            S3Repository.UPLOAD_RETRY_ENABLED,
            S3Repository.S3_PRIORITY_PERMIT_ALLOCATION_PERCENT,
            S3Repository.PERMIT_BACKED_TRANSFER_ENABLED
        );
    }

    @Override
    public void reload(Settings settings) {
        // secure settings should be readable
        final Map<String, S3ClientSettings> clientsSettings = S3ClientSettings.load(settings, configPath);
        service.refreshAndClearCache(clientsSettings);
        s3AsyncService.refreshAndClearCache(clientsSettings);
    }

    @Override
    public void close() throws IOException {
        service.close();
        s3AsyncService.close();
    }
}
