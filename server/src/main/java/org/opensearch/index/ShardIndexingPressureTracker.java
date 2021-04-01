/*
 * Copyright OpenSearch Contributors.
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.index;

import org.opensearch.index.shard.ShardId;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class contains all the tracking objects that will be maintained against a shard and will be used and modified
 * while evaluating shard indexing pressure related information for a shard.
 *
 * This class tracks these parameters at coordinating, primary and replica indexing stage.
 * 1. CurrentBytes - Bytes of data that is inflight/processing for a shard.
 * 2. TotalBytes - Total bytes that was processed successfully for a shard.
 * 3. Counts - Total number of requests that were processed successfully for a shard.
 * 4. TimeInMillis - Total indexing time take by requests that were processed successfully for a shard.
 * 5. Rejections - Total number of requests that were rejected for a shard.
 * 6. NodeLimitsBreachedRejections - Total number of requests that were rejected due to the node level limits breached.
 *                                   i.e. when a request for a shard came and there was no scope for the shard to grow as
 *                                   node level limit was already reached.
 * 7. LastSuccessfulRequestLimitsBreachedRejections - Total number of requests that were rejected due to the
 *                                                    last successful request limits breached for a shard.
 * 8. ThroughputDegradationLimitsBreachedRejections - Total number of requests that were rejected due to the
 *                                                   last successful request limits breached for a shard.
 * 9. LastSuccessfulRequestTimestamp - Timestamp of last successful request for a shard.
 * 10. TotalOutstandingRequests - At any given point how many requests are outstanding for a shard.
 * 11. ThroughputMovingAverage - Hold the average throughput value for last N requests.
 * 12. ThroughputMovingQueue - Queue that holds the last N requests throughput such that we have a sliding window
 *                             which keeps moving everytime a new request comes such that at any given point we are looking
 *                             at last N requests only. EWMA cannot be used here as it evaluate the historical average
 *                             and here we need the average of just last N requests.
 *
 * For more details on 6,7,8,9,10,11 see {@link ShardIndexingPressureMemoryManager}
 */
public class ShardIndexingPressureTracker {

    //Memory trackers
    public final AtomicLong currentCombinedCoordinatingAndPrimaryBytes = new AtomicLong();
    public final AtomicLong currentCoordinatingBytes = new AtomicLong();
    public final AtomicLong currentPrimaryBytes = new AtomicLong();
    public final AtomicLong currentReplicaBytes = new AtomicLong();

    public final AtomicLong totalCombinedCoordinatingAndPrimaryBytes = new AtomicLong();
    public final AtomicLong totalCoordinatingBytes = new AtomicLong();
    public final AtomicLong totalPrimaryBytes = new AtomicLong();
    public final AtomicLong totalReplicaBytes = new AtomicLong();

    //Count based trackers
    public final AtomicLong coordinatingCount = new AtomicLong();
    public final AtomicLong primaryCount = new AtomicLong();
    public final AtomicLong replicaCount = new AtomicLong();

    //Latency
    public AtomicLong coordinatingTimeInMillis = new AtomicLong();
    public AtomicLong primaryTimeInMillis = new AtomicLong();
    public AtomicLong replicaTimeInMillis = new AtomicLong();

    //Coordinating Rejection Count
    public final AtomicLong coordinatingRejections = new AtomicLong();
    public final AtomicLong coordinatingNodeLimitsBreachedRejections = new AtomicLong();
    public final AtomicLong coordinatingLastSuccessfulRequestLimitsBreachedRejections = new AtomicLong();
    public final AtomicLong coordinatingThroughputDegradationLimitsBreachedRejections = new AtomicLong();

    //Primary Rejection Count
    public final AtomicLong primaryRejections = new AtomicLong();
    public final AtomicLong primaryNodeLimitsBreachedRejections = new AtomicLong();
    public final AtomicLong primaryLastSuccessfulRequestLimitsBreachedRejections = new AtomicLong();
    public final AtomicLong primaryThroughputDegradationLimitsBreachedRejections = new AtomicLong();

    //Replica Rejection Count
    public final AtomicLong replicaRejections = new AtomicLong();
    public final AtomicLong replicaNodeLimitsBreachedRejections = new AtomicLong();
    public final AtomicLong replicaLastSuccessfulRequestLimitsBreachedRejections = new AtomicLong();
    public final AtomicLong replicaThroughputDegradationLimitsBreachedRejections = new AtomicLong();

    //Last Successful TimeStamp
    public final AtomicLong lastSuccessfulCoordinatingRequestTimestamp = new AtomicLong();
    public final AtomicLong lastSuccessfulPrimaryRequestTimestamp = new AtomicLong();
    public final AtomicLong lastSuccessfulReplicaRequestTimestamp = new AtomicLong();

    //Total Outstanding requests after last successful request
    public final AtomicLong totalOutstandingCoordinatingRequests = new AtomicLong();
    public final AtomicLong totalOutstandingPrimaryRequests = new AtomicLong();
    public final AtomicLong totalOutstandingReplicaRequests = new AtomicLong();

    /*
    Shard Window Throughput Tracker.
    We will be using atomic long to track double values as mentioned here -
    https://docs.oracle.com/javase/6/docs/api/java/util/concurrent/atomic/package-summary.html
     */
    public final AtomicLong coordinatingThroughputMovingAverage = new AtomicLong();
    public final AtomicLong primaryThroughputMovingAverage = new AtomicLong();
    public final AtomicLong replicaThroughputMovingAverage = new AtomicLong();

    //Shard Window Throughput Queue
    public final ConcurrentLinkedQueue<Double> coordinatingThroughputMovingQueue = new ConcurrentLinkedQueue();
    public final ConcurrentLinkedQueue<Double> primaryThroughputMovingQueue = new ConcurrentLinkedQueue();
    public final ConcurrentLinkedQueue<Double> replicaThroughputMovingQueue = new ConcurrentLinkedQueue();

    //Shard Reference
    public final ShardId shardId;

    //Memory Allotted
    public final AtomicLong primaryAndCoordinatingLimits = new AtomicLong(0);
    public final AtomicLong replicaLimits = new AtomicLong(0);

    public ShardIndexingPressureTracker(ShardId shardId) {
        this.shardId = shardId;
    }
}
