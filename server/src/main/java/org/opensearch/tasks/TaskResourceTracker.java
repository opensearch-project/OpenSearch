/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tasks;

import com.sun.management.ThreadMXBean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.shard.ShardId;
import org.opensearch.watcher.ResourceWatcher;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class TaskResourceTracker implements ResourceWatcher {

    private static final Logger logger = LogManager.getLogger(TaskManager.class);

    /**
     * Single class instance
     */
    private static TaskResourceTracker instance = new TaskResourceTracker();

    public final ConcurrentHashMap<TaskInfoKey, List<TaskWorkerResourceUtilInfo>> taskMap;
    public final ConcurrentHashMap<Object, Long> overhead;
    private ThreadMXBean threadMXBean;
    private long searchBytes;
    private long responseOverheadBytes;

    /**
     * Private constructor to keep class Singleton
     */
    private TaskResourceTracker() {
        taskMap = new ConcurrentHashMap<>();
        threadMXBean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
        searchBytes = 0;
        responseOverheadBytes = 0;
        overhead = new ConcurrentHashMap<>();
    }

    /**
     * Method for returning static instance of class
     *
     * @return singleton instance for class
     */
    public static TaskResourceTracker getInstance() {
        return instance;
    }

    public void registerTaskForTracking(long taskId, List<String> indices, ShardId shardId, String actionName) {
        taskMap.put(new TaskInfoKey(taskId, indices, shardId, actionName), new ArrayList<>());
    }

    public void registerWorkerForTask(long taskId, long workerId, long cpuCurrent, long bytesCurrent, String threadpoolName) {
        // TODO remove this after identifying cases where it can be true
        if (taskMap.get(new TaskInfoKey(taskId)) == null) {
            return;
        }

        TaskWorkerResourceUtilInfo taskWorkerResourceUtilInfo =
            new TaskWorkerResourceUtilInfo(workerId, cpuCurrent, cpuCurrent, bytesCurrent, bytesCurrent,
                true, threadpoolName);

        taskMap.get(new TaskInfoKey(taskId)).add(taskWorkerResourceUtilInfo);
    }

    public void unregisterWorkerForTask(long taskId, long workerId, long cpuCurrent, long bytesCurrent) {
        // This happens when task is unregistered first and then the runnable finishes
        if (taskMap.get(new TaskInfoKey(taskId)) == null) {
            return;
        }
        TaskWorkerResourceUtilInfo taskWorkerResourceUtilInfo = taskMap.get(new TaskInfoKey(taskId)).stream()
            .filter(twrui -> twrui.getWorkerId() != null && twrui.getWorkerId() == workerId
                && twrui.isActive()).findFirst().get();
        taskWorkerResourceUtilInfo.setHeapNow(bytesCurrent);
        taskWorkerResourceUtilInfo.setCpuNow(cpuCurrent);
        taskWorkerResourceUtilInfo.setActive(false);
    }

    private void snapshot() {
        final long[] search = {0};
        final long[] response = {0};
        taskMap.forEach((taskInfoKey, taskWorkerResourceUtilInfos) -> taskWorkerResourceUtilInfos.forEach(taskWorkerResourceUtilInfo -> {
            if (taskWorkerResourceUtilInfo.isActive()) {
                taskWorkerResourceUtilInfo.setHeapNow(threadMXBean.getThreadAllocatedBytes(taskWorkerResourceUtilInfo.getWorkerId()));
            }
            if ("tw".equals(taskWorkerResourceUtilInfo.getThreadPoolName())) {
                response[0] += taskWorkerResourceUtilInfo.getOverheardBytes();
            } else {
                search[0] += taskWorkerResourceUtilInfo.getHeapNow() - taskWorkerResourceUtilInfo.getHeapStart();
            }
        }));

        searchBytes = search[0];
        responseOverheadBytes = response[0];
    }

    @Override
    public void init() throws IOException {
    }

    @Override
    public void checkAndNotify() throws IOException {
        logger.trace("Taking Scheduled snapshot for System resources");
        snapshot();
    }

    public void unregisterTaskForTracking(long id) {
        logger.trace("Unregistering task");
        // TODO if there's any active worker take snapshot of those and exit. (If accurate data required for a task)
        taskMap.remove(new TaskInfoKey(id));
    }

    public void registerResponseOverhead(long taskId, long bytes) {
        TaskWorkerResourceUtilInfo taskWorkerResourceUtilInfo =
            new TaskWorkerResourceUtilInfo(bytes);

        // TODO remove from overhead map as well

        taskMap.get(new TaskInfoKey(taskId)).add(taskWorkerResourceUtilInfo);
    }

    public void registerResponseOverhead1(Object ob, long bytes) {
        overhead.put(ob, bytes);
    }

    public void transfer(long taskId, Object ob, long bytes) {
        TaskInfoKey key = new TaskInfoKey(taskId);
        if (!overhead.containsKey(ob) || taskMap.get(key) == null) return;

        long bytesStart = overhead.get(ob);

        TaskWorkerResourceUtilInfo t = new TaskWorkerResourceUtilInfo(1L, 0L, 0L, bytesStart, bytes, false, "tw");
        taskMap.get(key).add(t);
        overhead.remove(ob);
    }

    public long getBytes() {
        return searchBytes;
    }

    public long getResponseOverhead() {
        return responseOverheadBytes;
    }
}
