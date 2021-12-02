/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tasks;

public class TaskWorkerResourceUtilInfo {

    private Long workerId;
    private Long cpuStart;
    private Long cpuNow;
    private Long heapStart;
    private Long heapNow;
    private boolean active;
    private String threadPoolName;

    public long getOverheardBytes() {
        return overheardBytes;
    }

    private long overheardBytes;

    public TaskWorkerResourceUtilInfo(Long workerId, Long cpuStart, Long cpuNow, Long heapStart, Long heapNow, boolean active, String threadPoolName) {
        this.workerId = workerId;
        this.cpuStart = cpuStart;
        this.cpuNow = cpuNow;
        this.heapStart = heapStart;
        this.heapNow = heapNow;
        this.active = active;
        this.threadPoolName = threadPoolName;
    }

    public TaskWorkerResourceUtilInfo(Long overhead) {
        overheardBytes = overhead;
    }

    public Long getWorkerId() {
        return workerId;
    }

    public void setWorkerId(Long workerId) {
        this.workerId = workerId;
    }

    public Long getCpuStart() {
        return cpuStart;
    }

    public void setCpuStart(Long cpuStart) {
        this.cpuStart = cpuStart;
    }

    public Long getCpuNow() {
        return cpuNow;
    }

    public void setCpuNow(Long cpuNow) {
        this.cpuNow = cpuNow;
    }

    public Long getHeapStart() {
        return heapStart;
    }

    public void setHeapStart(Long heapStart) {
        this.heapStart = heapStart;
    }

    public Long getHeapNow() {
        return heapNow;
    }

    public void setHeapNow(Long heapNow) {
        this.heapNow = heapNow;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public String getThreadPoolName() {
        return threadPoolName;
    }

    public void setThreadPoolName(String threadPoolName) {
        this.threadPoolName = threadPoolName;
    }
}
