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

package org.opensearch.threadpool;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.Version;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.SizeValue;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.common.util.concurrent.OpenSearchThreadPoolExecutor;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.util.concurrent.XRejectedExecutionHandler;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.service.ReportingService;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.gateway.remote.ClusterStateChecksum;
import org.opensearch.node.Node;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableMap;

/**
 * The OpenSearch threadpool class
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class ThreadPool implements ReportingService<ThreadPoolInfo>, Scheduler {

    private static final Logger logger = LogManager.getLogger(ThreadPool.class);

    /**
     * The threadpool names.
     *
     * @opensearch.internal
     */
    public static class Names {
        public static final String SAME = "same";
        public static final String GENERIC = "generic";
        @Deprecated
        public static final String LISTENER = "listener";
        public static final String GET = "get";
        public static final String ANALYZE = "analyze";
        public static final String WRITE = "write";
        public static final String SEARCH = "search";
        public static final String SEARCH_THROTTLED = "search_throttled";
        public static final String MANAGEMENT = "management";
        public static final String FLUSH = "flush";
        public static final String REFRESH = "refresh";
        public static final String WARMER = "warmer";
        public static final String SNAPSHOT = "snapshot";
        public static final String SNAPSHOT_DELETION = "snapshot_deletion";
        public static final String FORCE_MERGE = "force_merge";
        public static final String FETCH_SHARD_STARTED = "fetch_shard_started";
        public static final String FETCH_SHARD_STORE = "fetch_shard_store";
        public static final String SYSTEM_READ = "system_read";
        public static final String SYSTEM_WRITE = "system_write";
        public static final String TRANSLOG_TRANSFER = "translog_transfer";
        public static final String TRANSLOG_SYNC = "translog_sync";
        public static final String REMOTE_PURGE = "remote_purge";
        public static final String REMOTE_REFRESH_RETRY = "remote_refresh_retry";
        public static final String REMOTE_RECOVERY = "remote_recovery";
        public static final String REMOTE_STATE_READ = "remote_state_read";
        public static final String INDEX_SEARCHER = "index_searcher";
        public static final String REMOTE_STATE_CHECKSUM = "remote_state_checksum";
    }

    static Set<String> scalingThreadPoolKeys = new HashSet<>(Arrays.asList("max", "core"));
    static Set<String> fixedThreadPoolKeys = new HashSet<>(Arrays.asList("size"));

    /**
     * The threadpool type.
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public enum ThreadPoolType {
        DIRECT("direct"),
        FIXED("fixed"),
        RESIZABLE("resizable"),
        SCALING("scaling");

        private final String type;

        public String getType() {
            return type;
        }

        ThreadPoolType(String type) {
            this.type = type;
        }

        private static final Map<String, ThreadPoolType> TYPE_MAP;

        static {
            Map<String, ThreadPoolType> typeMap = new HashMap<>();
            for (ThreadPoolType threadPoolType : ThreadPoolType.values()) {
                typeMap.put(threadPoolType.getType(), threadPoolType);
            }
            TYPE_MAP = Collections.unmodifiableMap(typeMap);
        }

        public static ThreadPoolType fromType(String type) {
            ThreadPoolType threadPoolType = TYPE_MAP.get(type);
            if (threadPoolType == null) {
                throw new IllegalArgumentException("no ThreadPoolType for " + type);
            }
            return threadPoolType;
        }
    }

    public static final Map<String, ThreadPoolType> THREAD_POOL_TYPES;

    static {
        HashMap<String, ThreadPoolType> map = new HashMap<>();
        map.put(Names.SAME, ThreadPoolType.DIRECT);
        map.put(Names.GENERIC, ThreadPoolType.SCALING);
        map.put(Names.LISTENER, ThreadPoolType.FIXED);
        map.put(Names.GET, ThreadPoolType.FIXED);
        map.put(Names.ANALYZE, ThreadPoolType.FIXED);
        map.put(Names.WRITE, ThreadPoolType.FIXED);
        map.put(Names.SEARCH, ThreadPoolType.RESIZABLE);
        map.put(Names.MANAGEMENT, ThreadPoolType.SCALING);
        map.put(Names.FLUSH, ThreadPoolType.SCALING);
        map.put(Names.REFRESH, ThreadPoolType.SCALING);
        map.put(Names.WARMER, ThreadPoolType.SCALING);
        map.put(Names.SNAPSHOT, ThreadPoolType.SCALING);
        map.put(Names.SNAPSHOT_DELETION, ThreadPoolType.SCALING);
        map.put(Names.FORCE_MERGE, ThreadPoolType.FIXED);
        map.put(Names.FETCH_SHARD_STARTED, ThreadPoolType.SCALING);
        map.put(Names.FETCH_SHARD_STORE, ThreadPoolType.SCALING);
        map.put(Names.SEARCH_THROTTLED, ThreadPoolType.RESIZABLE);
        map.put(Names.SYSTEM_READ, ThreadPoolType.FIXED);
        map.put(Names.SYSTEM_WRITE, ThreadPoolType.FIXED);
        map.put(Names.TRANSLOG_TRANSFER, ThreadPoolType.SCALING);
        map.put(Names.TRANSLOG_SYNC, ThreadPoolType.FIXED);
        map.put(Names.REMOTE_PURGE, ThreadPoolType.SCALING);
        map.put(Names.REMOTE_REFRESH_RETRY, ThreadPoolType.SCALING);
        map.put(Names.REMOTE_RECOVERY, ThreadPoolType.SCALING);
        map.put(Names.REMOTE_STATE_READ, ThreadPoolType.SCALING);
        map.put(Names.INDEX_SEARCHER, ThreadPoolType.RESIZABLE);
        map.put(Names.REMOTE_STATE_CHECKSUM, ThreadPoolType.FIXED);
        THREAD_POOL_TYPES = Collections.unmodifiableMap(map);
    }

    private final Map<String, ExecutorHolder> executors;

    private final ThreadPoolInfo threadPoolInfo;

    private final CachedTimeThread cachedTimeThread;

    static final ExecutorService DIRECT_EXECUTOR = OpenSearchExecutors.newDirectExecutorService();

    private final ThreadContext threadContext;

    private final Map<String, ExecutorBuilder> builders;

    private final ScheduledThreadPoolExecutor scheduler;

    public Collection<ExecutorBuilder> builders() {
        return Collections.unmodifiableCollection(builders.values());
    }

    public static Setting<TimeValue> ESTIMATED_TIME_INTERVAL_SETTING = Setting.timeSetting(
        "thread_pool.estimated_time_interval",
        TimeValue.timeValueMillis(200),
        TimeValue.ZERO,
        Setting.Property.NodeScope
    );

    public static final Setting<Settings> CLUSTER_THREAD_POOL_SIZE_SETTING = Setting.groupSetting(
        "cluster.thread_pool.",
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public ThreadPool(final Settings settings, final ExecutorBuilder<?>... customBuilders) {
        this(settings, null, customBuilders);
    }

    public ThreadPool(
        final Settings settings,
        final AtomicReference<RunnableTaskExecutionListener> runnableTaskListener,
        final ExecutorBuilder<?>... customBuilders
    ) {
        assert Node.NODE_NAME_SETTING.exists(settings);

        final Map<String, ExecutorBuilder> builders = new HashMap<>();
        final int allocatedProcessors = OpenSearchExecutors.allocatedProcessors(settings);
        final int halfProc = halfAllocatedProcessors(allocatedProcessors);
        final int halfProcMaxAt5 = halfAllocatedProcessorsMaxFive(allocatedProcessors);
        final int halfProcMaxAt10 = halfAllocatedProcessorsMaxTen(allocatedProcessors);
        final int genericThreadPoolMax = boundedBy(4 * allocatedProcessors, 128, 512);
        final int snapshotDeletionPoolMax = boundedBy(4 * allocatedProcessors, 64, 256);
        builders.put(Names.GENERIC, new ScalingExecutorBuilder(Names.GENERIC, 4, genericThreadPoolMax, TimeValue.timeValueSeconds(30)));
        builders.put(Names.WRITE, new FixedExecutorBuilder(settings, Names.WRITE, allocatedProcessors, 10000));
        builders.put(Names.GET, new FixedExecutorBuilder(settings, Names.GET, allocatedProcessors, 1000));
        builders.put(Names.ANALYZE, new FixedExecutorBuilder(settings, Names.ANALYZE, 1, 16));
        builders.put(
            Names.SEARCH,
            new ResizableExecutorBuilder(settings, Names.SEARCH, searchThreadPoolSize(allocatedProcessors), 1000, runnableTaskListener)
        );
        builders.put(Names.SEARCH_THROTTLED, new ResizableExecutorBuilder(settings, Names.SEARCH_THROTTLED, 1, 100, runnableTaskListener));
        builders.put(Names.MANAGEMENT, new ScalingExecutorBuilder(Names.MANAGEMENT, 1, 5, TimeValue.timeValueMinutes(5)));
        // no queue as this means clients will need to handle rejections on listener queue even if the operation succeeded
        // the assumption here is that the listeners should be very lightweight on the listeners side
        builders.put(Names.LISTENER, new FixedExecutorBuilder(settings, Names.LISTENER, halfProcMaxAt10, -1, true));
        builders.put(Names.FLUSH, new ScalingExecutorBuilder(Names.FLUSH, 1, halfProcMaxAt5, TimeValue.timeValueMinutes(5)));
        builders.put(Names.REFRESH, new ScalingExecutorBuilder(Names.REFRESH, 1, halfProcMaxAt10, TimeValue.timeValueMinutes(5)));
        builders.put(Names.WARMER, new ScalingExecutorBuilder(Names.WARMER, 1, halfProcMaxAt5, TimeValue.timeValueMinutes(5)));
        builders.put(Names.SNAPSHOT, new ScalingExecutorBuilder(Names.SNAPSHOT, 1, halfProcMaxAt5, TimeValue.timeValueMinutes(5)));
        builders.put(
            Names.SNAPSHOT_DELETION,
            new ScalingExecutorBuilder(Names.SNAPSHOT_DELETION, 1, snapshotDeletionPoolMax, TimeValue.timeValueMinutes(5))
        );
        builders.put(
            Names.FETCH_SHARD_STARTED,
            new ScalingExecutorBuilder(Names.FETCH_SHARD_STARTED, 1, 2 * allocatedProcessors, TimeValue.timeValueMinutes(5))
        );
        builders.put(Names.FORCE_MERGE, new FixedExecutorBuilder(settings, Names.FORCE_MERGE, 1, -1));
        builders.put(
            Names.FETCH_SHARD_STORE,
            new ScalingExecutorBuilder(Names.FETCH_SHARD_STORE, 1, 2 * allocatedProcessors, TimeValue.timeValueMinutes(5))
        );
        builders.put(Names.SYSTEM_READ, new FixedExecutorBuilder(settings, Names.SYSTEM_READ, halfProcMaxAt5, 2000, false));
        builders.put(Names.SYSTEM_WRITE, new FixedExecutorBuilder(settings, Names.SYSTEM_WRITE, halfProcMaxAt5, 1000, false));
        builders.put(
            Names.TRANSLOG_TRANSFER,
            new ScalingExecutorBuilder(Names.TRANSLOG_TRANSFER, 1, halfProc, TimeValue.timeValueMinutes(5))
        );
        builders.put(Names.TRANSLOG_SYNC, new FixedExecutorBuilder(settings, Names.TRANSLOG_SYNC, allocatedProcessors * 4, 10000));
        builders.put(Names.REMOTE_PURGE, new ScalingExecutorBuilder(Names.REMOTE_PURGE, 1, halfProc, TimeValue.timeValueMinutes(5)));
        builders.put(
            Names.REMOTE_REFRESH_RETRY,
            new ScalingExecutorBuilder(Names.REMOTE_REFRESH_RETRY, 1, halfProc, TimeValue.timeValueMinutes(5))
        );
        builders.put(
            Names.REMOTE_RECOVERY,
            new ScalingExecutorBuilder(
                Names.REMOTE_RECOVERY,
                1,
                twiceAllocatedProcessors(allocatedProcessors),
                TimeValue.timeValueMinutes(5)
            )
        );
        builders.put(
            Names.REMOTE_STATE_READ,
            new ScalingExecutorBuilder(Names.REMOTE_STATE_READ, 1, boundedBy(4 * allocatedProcessors, 4, 32), TimeValue.timeValueMinutes(5))
        );
        builders.put(
            Names.INDEX_SEARCHER,
            new ResizableExecutorBuilder(
                settings,
                Names.INDEX_SEARCHER,
                twiceAllocatedProcessors(allocatedProcessors),
                1000,
                runnableTaskListener
            )
        );
        builders.put(
            Names.REMOTE_STATE_CHECKSUM,
            new FixedExecutorBuilder(settings, Names.REMOTE_STATE_CHECKSUM, ClusterStateChecksum.COMPONENT_SIZE, 1000)
        );

        for (final ExecutorBuilder<?> builder : customBuilders) {
            if (builders.containsKey(builder.name())) {
                throw new IllegalArgumentException("builder with name [" + builder.name() + "] already exists");
            }
            builders.put(builder.name(), builder);
        }
        this.builders = Collections.unmodifiableMap(builders);

        threadContext = new ThreadContext(settings);

        final Map<String, ExecutorHolder> executors = new HashMap<>();
        for (final Map.Entry<String, ExecutorBuilder> entry : builders.entrySet()) {
            final ExecutorBuilder.ExecutorSettings executorSettings = entry.getValue().getSettings(settings);
            final ExecutorHolder executorHolder = entry.getValue().build(executorSettings, threadContext);
            if (executors.containsKey(executorHolder.info.getName())) {
                throw new IllegalStateException("duplicate executors with name [" + executorHolder.info.getName() + "] registered");
            }
            logger.debug("created thread pool: {}", entry.getValue().formatInfo(executorHolder.info));
            executors.put(entry.getKey(), executorHolder);
        }

        executors.put(Names.SAME, new ExecutorHolder(DIRECT_EXECUTOR, new Info(Names.SAME, ThreadPoolType.DIRECT)));
        this.executors = unmodifiableMap(executors);

        final List<Info> infos = executors.values()
            .stream()
            .filter(holder -> holder.info.getName().equals("same") == false)
            .map(holder -> holder.info)
            .collect(Collectors.toList());
        this.threadPoolInfo = new ThreadPoolInfo(infos);
        this.scheduler = Scheduler.initScheduler(settings);
        TimeValue estimatedTimeInterval = ESTIMATED_TIME_INTERVAL_SETTING.get(settings);
        this.cachedTimeThread = new CachedTimeThread(OpenSearchExecutors.threadName(settings, "[timer]"), estimatedTimeInterval.millis());
        this.cachedTimeThread.start();
    }

    /**
     * Returns a value of milliseconds that may be used for relative time calculations.
     * <p>
     * This method should only be used for calculating time deltas. For an epoch based
     * timestamp, see {@link #absoluteTimeInMillis()}.
     */
    public long relativeTimeInMillis() {
        return TimeValue.nsecToMSec(relativeTimeInNanos());
    }

    /**
     * Returns a value of nanoseconds that may be used for relative time calculations.
     * <p>
     * This method should only be used for calculating time deltas. For an epoch based
     * timestamp, see {@link #absoluteTimeInMillis()}.
     */
    public long relativeTimeInNanos() {
        return cachedTimeThread.relativeTimeInNanos();
    }

    /**
     * Returns a value of nanoseconds that may be used for relative time calculations
     * that require the highest precision possible. Performance critical code must use
     * either {@link #relativeTimeInNanos()} or {@link #relativeTimeInMillis()} which
     * give better performance at the cost of lower precision.
     * <p>
     * This method should only be used for calculating time deltas. For an epoch based
     * timestamp, see {@link #absoluteTimeInMillis()}.
     */
    public long preciseRelativeTimeInNanos() {
        return System.nanoTime();
    }

    /**
     * Returns the value of milliseconds since UNIX epoch.
     * <p>
     * This method should only be used for exact date/time formatting. For calculating
     * time deltas that should not suffer from negative deltas, which are possible with
     * this method, see {@link #relativeTimeInMillis()}.
     */
    public long absoluteTimeInMillis() {
        return cachedTimeThread.absoluteTimeInMillis();
    }

    @Override
    public ThreadPoolInfo info() {
        return threadPoolInfo;
    }

    public Info info(String name) {
        ExecutorHolder holder = executors.get(name);
        if (holder == null) {
            return null;
        }
        return holder.info;
    }

    public void registerClusterSettingsListeners(ClusterSettings clusterSettings) {
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_THREAD_POOL_SIZE_SETTING, this::setThreadPool, this::validateSetting);
    }

    /*
    Scaling threadpool can provide only max and core
    Fixed/ResizableQueue can provide only size

    For example valid settings would be for scaling and fixed thead pool
        cluster.threadpool.snapshot.max : "5",
        cluster.threadpool.snapshot.core : "5",
        cluster.threadpool.get.size : "2",
     */
    private void validateSetting(Settings tpSettings) {
        Map<String, Settings> tpGroups = tpSettings.getAsGroups();
        for (Map.Entry<String, Settings> entry : tpGroups.entrySet()) {
            String tpName = entry.getKey();
            if (THREAD_POOL_TYPES.containsKey(tpName) == false) {
                throw new IllegalArgumentException("illegal thread_pool name : " + tpName);
            }
            Settings tpGroup = entry.getValue();
            ExecutorHolder holder = executors.get(tpName);
            assert holder.executor instanceof OpenSearchThreadPoolExecutor;
            OpenSearchThreadPoolExecutor threadPoolExecutor = (OpenSearchThreadPoolExecutor) holder.executor;
            if (holder.info.type == ThreadPoolType.SCALING) {
                if (scalingThreadPoolKeys.containsAll(tpGroup.keySet()) == false) {
                    throw new IllegalArgumentException(
                        "illegal thread_pool config : " + tpGroup.keySet() + " should only have " + scalingThreadPoolKeys
                    );
                }
                int max = tpGroup.getAsInt("max", threadPoolExecutor.getMaximumPoolSize());
                int core = tpGroup.getAsInt("core", threadPoolExecutor.getCorePoolSize());
                if (core < 1 || max < 1) {
                    throw new IllegalArgumentException("illegal value for [cluster.thread_pool." + tpName + "], has to be positive value");
                } else if (core > max) {
                    throw new IllegalArgumentException("core threadpool size cannot be greater than max");
                }
            } else {
                if (fixedThreadPoolKeys.containsAll(tpGroup.keySet()) == false) {
                    throw new IllegalArgumentException(
                        "illegal thread_pool config : " + tpGroup.keySet() + " should only have " + fixedThreadPoolKeys
                    );
                }
                int size = tpGroup.getAsInt("size", threadPoolExecutor.getMaximumPoolSize());
                if (size < 1) {
                    throw new IllegalArgumentException("illegal value for [cluster.thread_pool." + tpName + "], has to be positive value");
                }
            }
        }
    }

    public void setThreadPool(Settings tpSettings) {
        Map<String, Settings> tpGroups = tpSettings.getAsGroups();
        for (Map.Entry<String, Settings> entry : tpGroups.entrySet()) {
            String tpName = entry.getKey();
            Settings tpGroup = entry.getValue();
            ExecutorHolder holder = executors.get(tpName);
            assert holder.executor instanceof OpenSearchThreadPoolExecutor;
            OpenSearchThreadPoolExecutor executor = (OpenSearchThreadPoolExecutor) holder.executor;
            if (holder.info.type == ThreadPoolType.SCALING) {
                int max = tpGroup.getAsInt("max", executor.getMaximumPoolSize());
                int core = tpGroup.getAsInt("core", executor.getCorePoolSize());
                /*
                 If we are decreasing, core pool size has to be decreased first.
                 If we are increasing ,max pool size has to be increased first
                 This ensures that core pool is always smaller than max pool size .
                 Other wise IllegalArgumentException will be thrown from ThreadPoolExecutor
                 */
                if (core < executor.getCorePoolSize()) {
                    executor.setCorePoolSize(core);
                    executor.setMaximumPoolSize(max);
                } else {
                    executor.setMaximumPoolSize(max);
                    executor.setCorePoolSize(core);
                }
            } else {
                int size = tpGroup.getAsInt("size", executor.getMaximumPoolSize());
                if (size < executor.getCorePoolSize()) {
                    executor.setCorePoolSize(size);
                    executor.setMaximumPoolSize(size);
                } else {
                    executor.setMaximumPoolSize(size);
                    executor.setCorePoolSize(size);
                }
            }
        }
    }

    public ThreadPoolStats stats() {
        List<ThreadPoolStats.Stats> stats = new ArrayList<>();
        for (ExecutorHolder holder : executors.values()) {
            final String name = holder.info.getName();
            // no need to have info on "same" thread pool
            if ("same".equals(name)) {
                continue;
            }
            int threads = -1;
            int queue = -1;
            int active = -1;
            long rejected = -1;
            int largest = -1;
            long completed = -1;
            long waitTimeNanos = -1;
            if (holder.executor() instanceof OpenSearchThreadPoolExecutor) {
                OpenSearchThreadPoolExecutor threadPoolExecutor = (OpenSearchThreadPoolExecutor) holder.executor();
                threads = threadPoolExecutor.getPoolSize();
                queue = threadPoolExecutor.getQueue().size();
                active = threadPoolExecutor.getActiveCount();
                largest = threadPoolExecutor.getLargestPoolSize();
                completed = threadPoolExecutor.getCompletedTaskCount();
                waitTimeNanos = threadPoolExecutor.getPoolWaitTimeNanos();
                RejectedExecutionHandler rejectedExecutionHandler = threadPoolExecutor.getRejectedExecutionHandler();
                if (rejectedExecutionHandler instanceof XRejectedExecutionHandler) {
                    rejected = ((XRejectedExecutionHandler) rejectedExecutionHandler).rejected();
                }
            }
            stats.add(new ThreadPoolStats.Stats(name, threads, queue, active, rejected, largest, completed, waitTimeNanos));
        }
        return new ThreadPoolStats(stats);
    }

    /**
     * Get the generic {@link ExecutorService}. This executor service
     * {@link Executor#execute(Runnable)} method will run the {@link Runnable} it is given in the
     * {@link ThreadContext} of the thread that queues it.
     * <p>
     * Warning: this {@linkplain ExecutorService} will not throw {@link RejectedExecutionException}
     * if you submit a task while it shutdown. It will instead silently queue it and not run it.
     */
    public ExecutorService generic() {
        return executor(Names.GENERIC);
    }

    /**
     * Get the {@link ExecutorService} with the given name. This executor service's
     * {@link Executor#execute(Runnable)} method will run the {@link Runnable} it is given in the
     * {@link ThreadContext} of the thread that queues it.
     * <p>
     * Warning: this {@linkplain ExecutorService} might not throw {@link RejectedExecutionException}
     * if you submit a task while it shutdown. It will instead silently queue it and not run it.
     *
     * @param name the name of the executor service to obtain
     * @throws IllegalArgumentException if no executor service with the specified name exists
     */
    public ExecutorService executor(String name) {
        final ExecutorHolder holder = executors.get(name);
        if (holder == null) {
            throw new IllegalArgumentException("no executor service found for [" + name + "]");
        }
        return holder.executor();
    }

    /**
     * Schedules a one-shot command to run after a given delay. The command is run in the context of the calling thread.
     *
     * @param command the command to run
     * @param delay delay before the task executes
     * @param executor the name of the thread pool on which to execute this task. SAME means "execute on the scheduler thread" which changes
     *        the meaning of the ScheduledFuture returned by this method. In that case the ScheduledFuture will complete only when the
     *        command completes.
     * @return a ScheduledFuture who's get will return when the task is has been added to its target thread pool and throw an exception if
     *         the task is canceled before it was added to its target thread pool. Once the task has been added to its target thread pool
     *         the ScheduledFuture will cannot interact with it.
     * @throws OpenSearchRejectedExecutionException if the task cannot be scheduled for execution
     */
    @Override
    public ScheduledCancellable schedule(Runnable command, TimeValue delay, String executor) {
        command = threadContext.preserveContext(command);
        if (!Names.SAME.equals(executor)) {
            command = new ThreadedRunnable(command, executor(executor));
        }
        return new ScheduledCancellableAdapter(scheduler.schedule(command, delay.millis(), TimeUnit.MILLISECONDS));
    }

    public void scheduleUnlessShuttingDown(TimeValue delay, String executor, Runnable command) {
        try {
            schedule(command, delay, executor);
        } catch (OpenSearchRejectedExecutionException e) {
            if (e.isExecutorShutdown()) {
                logger.debug(
                    new ParameterizedMessage(
                        "could not schedule execution of [{}] after [{}] on [{}] as executor is shut down",
                        command,
                        delay,
                        executor
                    ),
                    e
                );
            } else {
                throw e;
            }
        }
    }

    @Override
    public Cancellable scheduleWithFixedDelay(Runnable command, TimeValue interval, String executor) {
        return new ReschedulingRunnable(command, interval, executor, this, (e) -> {
            if (logger.isDebugEnabled()) {
                logger.debug(() -> new ParameterizedMessage("scheduled task [{}] was rejected on thread pool [{}]", command, executor), e);
            }
        },
            (e) -> logger.warn(
                () -> new ParameterizedMessage("failed to run scheduled task [{}] on thread pool [{}]", command, executor),
                e
            )
        );
    }

    protected final void stopCachedTimeThread() {
        cachedTimeThread.running = false;
        cachedTimeThread.interrupt();
    }

    public void shutdown() {
        stopCachedTimeThread();
        scheduler.shutdown();
        for (ExecutorHolder executor : executors.values()) {
            if (executor.executor() instanceof ThreadPoolExecutor) {
                executor.executor().shutdown();
            }
        }
    }

    public void shutdownNow() {
        stopCachedTimeThread();
        scheduler.shutdownNow();
        for (ExecutorHolder executor : executors.values()) {
            if (executor.executor() instanceof ThreadPoolExecutor) {
                executor.executor().shutdownNow();
            }
        }
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        boolean result = scheduler.awaitTermination(timeout, unit);
        for (ExecutorHolder executor : executors.values()) {
            if (executor.executor() instanceof ThreadPoolExecutor) {
                result &= executor.executor().awaitTermination(timeout, unit);
            }
        }
        cachedTimeThread.join(unit.toMillis(timeout));
        return result;
    }

    public ScheduledExecutorService scheduler() {
        return this.scheduler;
    }

    /**
     * Constrains a value between minimum and maximum values
     * (inclusive).
     *
     * @param value the value to constrain
     * @param min   the minimum acceptable value
     * @param max   the maximum acceptable value
     * @return min if value is less than min, max if value is greater
     * than value, otherwise value
     */
    static int boundedBy(int value, int min, int max) {
        return Math.min(max, Math.max(min, value));
    }

    static int halfAllocatedProcessors(int allocatedProcessors) {
        return (allocatedProcessors + 1) / 2;
    }

    static int halfAllocatedProcessorsMaxFive(final int allocatedProcessors) {
        return boundedBy((allocatedProcessors + 1) / 2, 1, 5);
    }

    static int halfAllocatedProcessorsMaxTen(final int allocatedProcessors) {
        return boundedBy((allocatedProcessors + 1) / 2, 1, 10);
    }

    static int twiceAllocatedProcessors(final int allocatedProcessors) {
        return boundedBy(2 * allocatedProcessors, 2, Integer.MAX_VALUE);
    }

    public static int searchThreadPoolSize(final int allocatedProcessors) {
        return ((allocatedProcessors * 3) / 2) + 1;
    }

    class LoggingRunnable implements Runnable {

        private final Runnable runnable;

        LoggingRunnable(Runnable runnable) {
            this.runnable = runnable;
        }

        @Override
        public void run() {
            try {
                runnable.run();
            } catch (Exception e) {
                logger.warn(() -> new ParameterizedMessage("failed to run {}", runnable.toString()), e);
                throw e;
            }
        }

        @Override
        public int hashCode() {
            return runnable.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return runnable.equals(obj);
        }

        @Override
        public String toString() {
            return "[threaded] " + runnable.toString();
        }
    }

    class ThreadedRunnable implements Runnable {

        private final Runnable runnable;

        private final Executor executor;

        ThreadedRunnable(Runnable runnable, Executor executor) {
            this.runnable = runnable;
            this.executor = executor;
        }

        @Override
        public void run() {
            try {
                executor.execute(runnable);
            } catch (OpenSearchRejectedExecutionException e) {
                if (e.isExecutorShutdown()) {
                    logger.debug(
                        new ParameterizedMessage(
                            "could not schedule execution of [{}] on [{}] as executor is shut down",
                            runnable,
                            executor
                        ),
                        e
                    );
                } else {
                    throw e;
                }
            }
        }

        @Override
        public int hashCode() {
            return runnable.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return runnable.equals(obj);
        }

        @Override
        public String toString() {
            return "[threaded] " + runnable.toString();
        }
    }

    /**
     * A thread to cache millisecond time values from
     * {@link System#nanoTime()} and {@link System#currentTimeMillis()}.
     * <p>
     * The values are updated at a specified interval.
     */
    static class CachedTimeThread extends Thread {

        final long interval;
        volatile boolean running = true;
        volatile long relativeNanos;
        volatile long absoluteMillis;

        CachedTimeThread(String name, long interval) {
            super(name);
            this.interval = interval;
            this.relativeNanos = System.nanoTime();
            this.absoluteMillis = System.currentTimeMillis();
            setDaemon(true);
        }

        /**
         * Return the current time used for relative calculations. This is {@link System#nanoTime()}.
         * <p>
         * If {@link ThreadPool#ESTIMATED_TIME_INTERVAL_SETTING} is set to 0
         * then the cache is disabled and the method calls {@link System#nanoTime()}
         * whenever called. Typically used for testing.
         */
        long relativeTimeInNanos() {
            if (0 < interval) {
                return relativeNanos;
            }
            return System.nanoTime();
        }

        /**
         * Return the current epoch time, used to find absolute time. This is
         * a cached version of {@link System#currentTimeMillis()}.
         * <p>
         * If {@link ThreadPool#ESTIMATED_TIME_INTERVAL_SETTING} is set to 0
         * then the cache is disabled and the method calls {@link System#currentTimeMillis()}
         * whenever called. Typically used for testing.
         */
        long absoluteTimeInMillis() {
            if (0 < interval) {
                return absoluteMillis;
            }
            return System.currentTimeMillis();
        }

        @Override
        public void run() {
            while (running && 0 < interval) {
                relativeNanos = System.nanoTime();
                absoluteMillis = System.currentTimeMillis();
                try {
                    Thread.sleep(interval);
                } catch (InterruptedException e) {
                    running = false;
                    return;
                }
            }
        }
    }

    static class ExecutorHolder {
        private final ExecutorService executor;
        public final Info info;

        ExecutorHolder(ExecutorService executor, Info info) {
            assert executor instanceof OpenSearchThreadPoolExecutor || executor == DIRECT_EXECUTOR;
            this.executor = executor;
            this.info = info;
        }

        ExecutorService executor() {
            return executor;
        }
    }

    /**
     * The thread pool information.
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class Info implements Writeable, ToXContentFragment {

        private final String name;
        private final ThreadPoolType type;
        private final int min;
        private final int max;
        private final TimeValue keepAlive;
        private final SizeValue queueSize;

        public Info(String name, ThreadPoolType type) {
            this(name, type, -1);
        }

        public Info(String name, ThreadPoolType type, int size) {
            this(name, type, size, size, null, null);
        }

        public Info(String name, ThreadPoolType type, int min, int max, @Nullable TimeValue keepAlive, @Nullable SizeValue queueSize) {
            this.name = name;
            this.type = type;
            this.min = min;
            this.max = max;
            this.keepAlive = keepAlive;
            this.queueSize = queueSize;
        }

        public Info(StreamInput in) throws IOException {
            name = in.readString();
            final String typeStr = in.readString();
            // Opensearch on or after 3.0.0 version doesn't know about "fixed_auto_queue_size" thread pool. Convert it to RESIZABLE.
            if (typeStr.equalsIgnoreCase("fixed_auto_queue_size")) {
                type = ThreadPoolType.RESIZABLE;
            } else {
                type = ThreadPoolType.fromType(typeStr);
            }
            min = in.readInt();
            max = in.readInt();
            keepAlive = in.readOptionalTimeValue();
            queueSize = in.readOptionalWriteable(SizeValue::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            if (type == ThreadPoolType.RESIZABLE && out.getVersion().before(Version.V_3_0_0)) {
                // Opensearch on older version doesn't know about "resizable" thread pool. Convert RESIZABLE to FIXED
                // to avoid serialization/de-serization issue between nodes with different OpenSearch version
                out.writeString(ThreadPoolType.FIXED.getType());
            } else {
                out.writeString(type.getType());
            }
            out.writeInt(min);
            out.writeInt(max);
            out.writeOptionalTimeValue(keepAlive);
            out.writeOptionalWriteable(queueSize);
        }

        public String getName() {
            return this.name;
        }

        public ThreadPoolType getThreadPoolType() {
            return this.type;
        }

        public int getMin() {
            return this.min;
        }

        public int getMax() {
            return this.max;
        }

        @Nullable
        public TimeValue getKeepAlive() {
            return this.keepAlive;
        }

        @Nullable
        public SizeValue getQueueSize() {
            return this.queueSize;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(name);
            builder.field("type", type.getType());

            if (type == ThreadPoolType.SCALING) {
                assert min != -1;
                builder.field("core", min);
                assert max != -1;
                builder.field("max", max);
            } else {
                assert max != -1;
                builder.field("size", max);
            }
            if (keepAlive != null) {
                builder.field("keep_alive", keepAlive.toString());
            }
            if (queueSize == null) {
                builder.field("queue_size", -1);
            } else {
                builder.field("queue_size", queueSize.singles());
            }
            builder.endObject();
            return builder;
        }

    }

    /**
     * Returns <code>true</code> if the given service was terminated successfully. If the termination timed out,
     * the service is <code>null</code> this method will return <code>false</code>.
     */
    public static boolean terminate(ExecutorService service, long timeout, TimeUnit timeUnit) {
        if (service != null) {
            service.shutdown();
            if (awaitTermination(service, timeout, timeUnit)) return true;
            service.shutdownNow();
            return awaitTermination(service, timeout, timeUnit);
        }
        return false;
    }

    private static boolean awaitTermination(final ExecutorService service, final long timeout, final TimeUnit timeUnit) {
        try {
            if (service.awaitTermination(timeout, timeUnit)) {
                return true;
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return false;
    }

    /**
     * Returns <code>true</code> if the given pool was terminated successfully. If the termination timed out,
     * the service is <code>null</code> this method will return <code>false</code>.
     */
    public static boolean terminate(ThreadPool pool, long timeout, TimeUnit timeUnit) {
        if (pool != null) {
            // Leverage try-with-resources to close the threadpool
            pool.shutdown();
            if (awaitTermination(pool, timeout, timeUnit)) {
                return true;
            }
            // last resort
            pool.shutdownNow();
            return awaitTermination(pool, timeout, timeUnit);
        }
        return false;
    }

    private static boolean awaitTermination(final ThreadPool threadPool, final long timeout, final TimeUnit timeUnit) {
        try {
            if (threadPool.awaitTermination(timeout, timeUnit)) {
                return true;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return false;
    }

    public ThreadContext getThreadContext() {
        return threadContext;
    }

    public static boolean assertNotScheduleThread(String reason) {
        assert Thread.currentThread().getName().contains("scheduler") == false : "Expected current thread ["
            + Thread.currentThread()
            + "] to not be the scheduler thread. Reason: ["
            + reason
            + "]";
        return true;
    }

    public static boolean assertCurrentMethodIsNotCalledRecursively() {
        final StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
        assert stackTraceElements.length >= 3 : stackTraceElements.length;
        assert stackTraceElements[0].getMethodName().equals("getStackTrace") : stackTraceElements[0];
        assert stackTraceElements[1].getMethodName().equals("assertCurrentMethodIsNotCalledRecursively") : stackTraceElements[1];
        final StackTraceElement testingMethod = stackTraceElements[2];
        for (int i = 3; i < stackTraceElements.length; i++) {
            assert stackTraceElements[i].getClassName().equals(testingMethod.getClassName()) == false
                || stackTraceElements[i].getMethodName().equals(testingMethod.getMethodName()) == false : testingMethod.getClassName()
                    + "#"
                    + testingMethod.getMethodName()
                    + " is called recursively";
        }
        return true;
    }
}
