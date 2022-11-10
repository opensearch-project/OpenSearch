/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.jobscheduler.scheduler;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Job index, id and jobInfo mapping.
 */
public class ScheduledJobInfo {
    private Map<String, Map<String, JobSchedulingInfo>> jobInfoMap;

    ScheduledJobInfo() {
        this.jobInfoMap = new ConcurrentHashMap<>();
    }

    public Map<String, JobSchedulingInfo> getJobsByIndex(String indexName) {
        if(!this.jobInfoMap.containsKey(indexName)) {
            synchronized (this.jobInfoMap) {
                if(!this.jobInfoMap.containsKey(indexName)) {
                    this.jobInfoMap.put(indexName, new ConcurrentHashMap<>());
                }
            }
        }
        return this.jobInfoMap.get(indexName);
    }

    public JobSchedulingInfo getJobInfo(String indexName, String jobId) {
        return getJobsByIndex(indexName).get(jobId);
    }

    public void addJob(String indexName, String jobId, JobSchedulingInfo jobInfo) {
        if(!this.jobInfoMap.containsKey(indexName)) {
            synchronized (this.jobInfoMap) {
                if(!this.jobInfoMap.containsKey(indexName)) {
                    jobInfoMap.put(indexName, new ConcurrentHashMap<>());
                }
            }
        }

        this.jobInfoMap.get(indexName).put(jobId, jobInfo);
    }

    public JobSchedulingInfo removeJob(String indexName, String jobId) {
        if(this.jobInfoMap.containsKey(indexName)) {
            return this.jobInfoMap.get(indexName).remove(jobId);
        }

        return null;
    }
}
