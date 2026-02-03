/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.SetOnce;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.AbstractAsyncTask;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.common.util.concurrent.ConcurrentMapLong;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.management.LockInfo;
import java.lang.management.ManagementFactory;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

import static org.opensearch.env.Environment.PATH_LOGS_SETTING;

/**
 * Used for dump stack.
 */
@PublicApi(since = "3.5.0")
public class StackDumper extends AbstractAsyncTask {
    private static final Logger logger = LogManager.getLogger(StackDumper.class);
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.ROOT)
        .withZone(ZoneId.systemDefault());
    public static final String SLOW_TASK_DIR_NAME = "_slow";
    public static final String STACK_FILE_EXTENSION = ".stack";
    private final SetOnce<Path> directory = new SetOnce<>();
    private final ConcurrentMapLong<Task> trackedTasks = ConcurrentCollections.newConcurrentMapLongWithAggressiveConcurrency();
    private volatile int maxStackTraceDepth;
    private volatile boolean taskDumpEnabled;
    private volatile long taskStackDumpThresholdNano;
    private volatile long taskStackDumpFreqNano;
    private long lastDumpTimeNano = 0;
    private int dumpNumber = 0;

    /**
     * Whether to enable task stack dump.
     */
    public static final Setting<Boolean> TASK_STACK_DUMP_ENABLED = Setting.boolSetting(
        "task.stack_dump.enabled",
        false,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * The dump interval for task stack dump.
     */
    public static final Setting<TimeValue> TASK_STACK_DUMP_INTERVAL = Setting.timeSetting(
        "task.stack_dump.interval",
        TimeValue.timeValueSeconds(5),
        TimeValue.timeValueMillis(100),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * The dump threshold for task stack dump.
     */
    public static final Setting<TimeValue> TASK_STACK_DUMP_THRESHOLD = Setting.timeSetting(
        "task.stack_dump.threshold",
        TimeValue.timeValueSeconds(10),
        TimeValue.timeValueMillis(100),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * The max stack trace depth for task stack dump.
     */
    public static final Setting<Integer> TASK_STACK_DUMP_MAX_DEPTH = Setting.intSetting(
        "task.stack_dump.stacktrace_max_depth",
        200,
        1,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * The dump frequency for task stack dump.
     */
    public static final Setting<TimeValue> TASK_STACK_DUMP_FREQ = Setting.timeSetting(
        "task.stack_dump.dump_freq",
        TimeValue.timeValueMinutes(60),
        TimeValue.timeValueSeconds(10),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public StackDumper(ClusterSettings clusterSettings, Settings settings, ThreadPool threadPool) {
        super(logger, threadPool, TASK_STACK_DUMP_INTERVAL.get(settings), true);
        clusterSettings.addSettingsUpdateConsumer(TASK_STACK_DUMP_INTERVAL, this::setInterval);

        maxStackTraceDepth = TASK_STACK_DUMP_MAX_DEPTH.get(settings);
        clusterSettings.addSettingsUpdateConsumer(TASK_STACK_DUMP_MAX_DEPTH, value -> maxStackTraceDepth = value);
        this.taskDumpEnabled = TASK_STACK_DUMP_ENABLED.get(settings);
        clusterSettings.addSettingsUpdateConsumer(TASK_STACK_DUMP_ENABLED, this::setTaskDumpEnabled);
        taskStackDumpThresholdNano = TASK_STACK_DUMP_THRESHOLD.get(settings).nanos();
        clusterSettings.addSettingsUpdateConsumer(TASK_STACK_DUMP_THRESHOLD, this::setTaskStackDumpThresholdNano);
        taskStackDumpFreqNano = TASK_STACK_DUMP_FREQ.get(settings).nanos();
        clusterSettings.addSettingsUpdateConsumer(TASK_STACK_DUMP_FREQ, this::setTaskStackDumpFreqNano);

        Path logPath = Path.of(PATH_LOGS_SETTING.get(settings));
        if (this.directory.trySet(logPath.resolve(SLOW_TASK_DIR_NAME))) {
            if (!Files.exists(directory.get()) || !Files.isDirectory(directory.get())) {
                try {
                    Files.createDirectories(directory.get());
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        } else {
            logger.warn("Failed to create slow task dump directory: {}", directory.get());
        }
    }

    public void addTask(Task task) {
        if (taskDumpEnabled) {
            trackedTasks.put(task.getId(), task);
        }
    }

    public void removeTask(Task task) {
        trackedTasks.remove(task.getId());
    }

    @Override
    public boolean mustReschedule() {
        return true;
    }

    @Override
    protected void runInternal() {
        if (taskDumpEnabled) {
            long currentTimeNano = System.nanoTime();
            for (Task task : trackedTasks.values()) {
                if (currentTimeNano - task.getStartTimeNanos() > taskStackDumpThresholdNano) {
                    StringBuilder threadDump = new StringBuilder(3000);
                    dumpContext(task, threadDump);
                    dumpStack(threadDump);
                    break;
                }
            }
        } else {
            cancel();
        }
    }

    public void setTaskDumpEnabled(Boolean taskDumpEnabled) {
        if (taskDumpEnabled == this.taskDumpEnabled) {
            return;
        }
        if (taskDumpEnabled) {
            this.rescheduleIfNecessary();
        } else {
            this.cancel();
            trackedTasks.clear();
        }
        this.taskDumpEnabled = taskDumpEnabled;
    }

    public void setTaskStackDumpThresholdNano(TimeValue taskStackDumpThreshold) {
        this.taskStackDumpThresholdNano = taskStackDumpThreshold.nanos();
    }

    public void setTaskStackDumpFreqNano(TimeValue taskStackDumpFreq) {
        this.taskStackDumpFreqNano = taskStackDumpFreq.nanos();
    }

    private synchronized void dumpStack(StringBuilder threadDump) {
        try {
            threadDump.append(DATE_FORMATTER.format(Instant.now())).append('\n');

            long currentTimeNano = System.nanoTime();
            if (currentTimeNano - lastDumpTimeNano < taskStackDumpFreqNano) {
                logger.debug("failed to dump slow task threads, interval is less than {}ms", taskStackDumpFreqNano / 1_000_000);
                return;
            }
            this.lastDumpTimeNano = System.nanoTime();
            ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
            for (ThreadInfo threadInfo : threadMXBean.dumpAllThreads(true, true)) {
                threadDump.append(dumpThreadInfo(threadInfo));
            }

            if (directory.get() != null) {
                // at most create 500 thread dump files.
                String fileName = ((dumpNumber++) % 500) + STACK_FILE_EXTENSION;
                Files.write(
                    directory.get().resolve(fileName),
                    threadDump.toString().getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE
                );
            } else {
                logger.warn("dumped threads on task slow:\n{}", threadDump);
            }

        } catch (IOException e) {
            logger.warn("failed to dump slow task threads", e);
            throw new UncheckedIOException(e);
        }
    }

    private void dumpContext(Task task, StringBuilder sb) {
        sb.append("Desc: ").append(task.getDescription()).append("\n").append("\n");
    }

    /**
     * According to the <a href="https://stackoverflow.com/questions/6827952/threadmxbean-dumpallthreads-maxdepth">...</a>
     */
    private String dumpThreadInfo(ThreadInfo info) {
        StringBuilder sb = new StringBuilder(200);
        sb.append("\"" + info.getThreadName() + "\"" + " Id=" + info.getThreadId() + " " + info.getThreadState());

        if (info.getLockName() != null) {
            sb.append(" on " + info.getLockName());
        }
        if (info.getLockOwnerName() != null) {
            sb.append(" owned by \"" + info.getLockOwnerName() + "\" Id=" + info.getLockOwnerId());
        }
        if (info.isSuspended()) {
            sb.append(" (suspended)");
        }
        if (info.isInNative()) {
            sb.append(" (in native)");
        }
        sb.append('\n');
        int i = 0;
        for (; i < info.getStackTrace().length && i < maxStackTraceDepth; i++) {
            StackTraceElement ste = info.getStackTrace()[i];
            sb.append("\tat " + ste.toString());
            sb.append('\n');
            if (i == 0 && info.getLockInfo() != null) {
                Thread.State ts = info.getThreadState();
                switch (ts) {
                    case BLOCKED:
                        sb.append("\t-  blocked on " + info.getLockInfo());
                        sb.append('\n');
                        break;
                    case WAITING:
                        sb.append("\t-  waiting on " + info.getLockInfo());
                        sb.append('\n');
                        break;
                    case TIMED_WAITING:
                        sb.append("\t-  waiting on " + info.getLockInfo());
                        sb.append('\n');
                        break;
                    default:
                }
            }

            for (MonitorInfo mi : info.getLockedMonitors()) {
                if (mi.getLockedStackDepth() == i) {
                    sb.append("\t-  locked " + mi);
                    sb.append('\n');
                }
            }
        }
        if (i < info.getStackTrace().length) {
            sb.append("\t...");
            sb.append('\n');
        }

        LockInfo[] locks = info.getLockedSynchronizers();
        if (locks.length > 0) {
            sb.append("\n\tNumber of locked synchronizers = " + locks.length);
            sb.append('\n');
            for (LockInfo li : locks) {
                sb.append("\t- " + li);
                sb.append('\n');
            }
        }
        sb.append('\n');
        return sb.toString();
    }

    @Override
    protected String getThreadPool() {
        return ThreadPool.Names.GENERIC;
    }

    Path getPathForSlowTaskDump() {
        return directory.get();
    }
}
