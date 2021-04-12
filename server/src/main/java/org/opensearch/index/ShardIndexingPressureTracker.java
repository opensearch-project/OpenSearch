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
 * 1. MemoryTracker
 * a. CurrentBytes - Bytes of data that is inflight/processing for a shard.
 * b. TotalBytes - Total bytes that was processed successfully for a shard.
 *
 * 2. CountTracker
 * a. Counts - Total number of requests that were processed successfully for a shard.
 *
 * 3. LatencyTracker
 * a. TimeInMillis - Total indexing time take by requests that were processed successfully for a shard.
 *
 * 4. RejectionTracker
 * a. TotalRejections - Total number of requests that were rejected for a shard.
 * b. NodeLimitsBreachedRejections - Total number of requests that were rejected due to the node level limits breached.
 *                                   i.e. when a request for a shard came and there was no scope for the shard to grow as
 *                                   node level limit was already reached.
 * c. LastSuccessfulRequestLimitsBreachedRejections - Total number of requests that were rejected due to the
 *                                                    last successful request limits breached for a shard.
 * d. ThroughputDegradationLimitsBreachedRejections - Total number of requests that were rejected due to the
 *                                                   last successful request limits breached for a shard.
 *
 * 5. TimeStampTracker
 * a. LastSuccessfulRequestTimestamp - Timestamp of last successful request for a shard.
 *
 * 6. OutstandingRequestTracker
 * a. TotalOutstandingRequests - At any given point how many requests are outstanding for a shard.
 *
 * 7. ThroughputTracker
 * a. ThroughputMovingAverage - Hold the average throughput value for last N requests.
 * b. ThroughputMovingQueue - Queue that holds the last N requests throughput such that we have a sliding window
 *                             which keeps moving everytime a new request comes such that at any given point we are looking
 *                             at last N requests only. EWMA cannot be used here as it evaluate the historical average
 *                             and here we need the average of just last N requests.
 *
 * see {@link ShardIndexingPressureMemoryManager}
 *
 * ShardIndexingPressureTracker is the construct to track all the write requests targeted for a ShardId on the node, across all possible
 * transport-actions i.e. Coordinator, Primary and Replica. Tracker is uniquely identified against a Shard-Id and contains explicit tracking
 * fields for different kind of tracking needs against a Shard-Id, such as in-flight Coordinating requests, Coordinator + Primary requests,
 * Primary requests and Replica requests.
 * Currently the knowledge of shard roles (such as primary vs replica) is not explicit to the tracker, and it tracks different values based
 * on the interaction hooks of the above layers, which are separate for different type of operations at the transport-action layers.
 *
 * There is room for introducing more unique identity to the trackers based on Shard-Role or Shard-Allocation-Id, but that will also
 * increase the complexity of handling shard-lister events and handling other scenarios such as request-draining etc.
 *
 * To prefer simplicity for now we have modelled by keeping explicit fields for different operation tracking, while tracker by itself is
 * agnostic of the role today. We can revisit this modelling in future as and when we add more tracking information here.
 */
public class ShardIndexingPressureTracker {

    private final ShardId shardId;
    private final MemoryTracker memoryTracker = new MemoryTracker();
    private final CountTracker countTracker = new CountTracker();
    private final LatencyTracker latencyTracker = new LatencyTracker();
    private final RejectionTracker rejectionTracker = new RejectionTracker();
    private final TimeStampTracker timeStampTracker = new TimeStampTracker();
    private final OutstandingRequestTracker outstandingRequestTracker = new OutstandingRequestTracker();
    private final ThroughputTracker throughputTracker = new ThroughputTracker();

    private final AtomicLong primaryAndCoordinatingLimits;
    private final AtomicLong replicaLimits;

    public ShardId getShardId() {
        return shardId;
    }

    public ShardIndexingPressureTracker(ShardId shardId, long primaryAndCoordinatingLimits, long replicaLimits) {
        this.shardId = shardId;
        this.primaryAndCoordinatingLimits = new AtomicLong(primaryAndCoordinatingLimits);
        this.replicaLimits = new AtomicLong(replicaLimits);
    }

    public MemoryTracker memory() {
        return memoryTracker;
    }

    public CountTracker count() {
        return countTracker;
    }

    public RejectionTracker rejection() {
        return rejectionTracker;
    }

    public LatencyTracker latency() {
        return latencyTracker;
    }

    public TimeStampTracker timeStamp() {
        return timeStampTracker;
    }

    public OutstandingRequestTracker outstandingRequest() {
        return outstandingRequestTracker;
    }

    public ThroughputTracker throughput() {
        return throughputTracker;
    }

    public AtomicLong getPrimaryAndCoordinatingLimits() {
        return primaryAndCoordinatingLimits;
    }

    public AtomicLong getReplicaLimits() {
        return replicaLimits;
    }

    //Memory tracker
    public static class MemoryTracker {
        private final AtomicLong currentCombinedCoordinatingAndPrimaryBytes = new AtomicLong();
        private final AtomicLong currentCoordinatingBytes = new AtomicLong();
        private final AtomicLong currentPrimaryBytes = new AtomicLong();
        private final AtomicLong currentReplicaBytes = new AtomicLong();

        private final AtomicLong totalCombinedCoordinatingAndPrimaryBytes = new AtomicLong();
        private final AtomicLong totalCoordinatingBytes = new AtomicLong();
        private final AtomicLong totalPrimaryBytes = new AtomicLong();
        private final AtomicLong totalReplicaBytes = new AtomicLong();

        public AtomicLong getCurrentCombinedCoordinatingAndPrimaryBytes() {
            return currentCombinedCoordinatingAndPrimaryBytes;
        }

        public AtomicLong getCurrentCoordinatingBytes() {
            return currentCoordinatingBytes;
        }

        public AtomicLong getCurrentPrimaryBytes() {
            return currentPrimaryBytes;
        }

        public AtomicLong getCurrentReplicaBytes() {
            return currentReplicaBytes;
        }

        public AtomicLong getTotalCombinedCoordinatingAndPrimaryBytes() {
            return totalCombinedCoordinatingAndPrimaryBytes;
        }

        public AtomicLong getTotalCoordinatingBytes() {
            return totalCoordinatingBytes;
        }

        public AtomicLong getTotalPrimaryBytes() {
            return totalPrimaryBytes;
        }

        public AtomicLong getTotalReplicaBytes() {
            return totalReplicaBytes;
        }
    }

    //Count based tracker
    public static class CountTracker {
        private final AtomicLong coordinatingCount = new AtomicLong();
        private final AtomicLong primaryCount = new AtomicLong();
        private final AtomicLong replicaCount = new AtomicLong();

        public AtomicLong getCoordinatingCount() {
            return coordinatingCount;
        }

        public AtomicLong getPrimaryCount() {
            return primaryCount;
        }

        public AtomicLong getReplicaCount() {
            return replicaCount;
        }
    }

    //Latency Tracker
    public static class LatencyTracker {
        private final AtomicLong coordinatingTimeInMillis = new AtomicLong();
        private final AtomicLong primaryTimeInMillis = new AtomicLong();
        private final AtomicLong replicaTimeInMillis = new AtomicLong();

        public AtomicLong getCoordinatingTimeInMillis() {
            return coordinatingTimeInMillis;
        }

        public AtomicLong getPrimaryTimeInMillis() {
            return primaryTimeInMillis;
        }

        public AtomicLong getReplicaTimeInMillis() {
            return replicaTimeInMillis;
        }
    }

    //Rejection Count tracker
    public static class RejectionTracker {
        //Coordinating Rejection Count
        private final AtomicLong coordinatingRejections = new AtomicLong();
        private final AtomicLong coordinatingNodeLimitsBreachedRejections = new AtomicLong();
        private final AtomicLong coordinatingLastSuccessfulRequestLimitsBreachedRejections = new AtomicLong();
        private final AtomicLong coordinatingThroughputDegradationLimitsBreachedRejections = new AtomicLong();

        //Primary Rejection Count
        private final AtomicLong primaryRejections = new AtomicLong();
        private final AtomicLong primaryNodeLimitsBreachedRejections = new AtomicLong();
        private final AtomicLong primaryLastSuccessfulRequestLimitsBreachedRejections = new AtomicLong();
        private final AtomicLong primaryThroughputDegradationLimitsBreachedRejections = new AtomicLong();

        //Replica Rejection Count
        private final AtomicLong replicaRejections = new AtomicLong();
        private final AtomicLong replicaNodeLimitsBreachedRejections = new AtomicLong();
        private final AtomicLong replicaLastSuccessfulRequestLimitsBreachedRejections = new AtomicLong();
        private final AtomicLong replicaThroughputDegradationLimitsBreachedRejections = new AtomicLong();

        public AtomicLong getCoordinatingRejections() {
            return coordinatingRejections;
        }

        public AtomicLong getCoordinatingNodeLimitsBreachedRejections() {
            return coordinatingNodeLimitsBreachedRejections;
        }

        public AtomicLong getCoordinatingLastSuccessfulRequestLimitsBreachedRejections() {
            return coordinatingLastSuccessfulRequestLimitsBreachedRejections;
        }

        public AtomicLong getCoordinatingThroughputDegradationLimitsBreachedRejections() {
            return coordinatingThroughputDegradationLimitsBreachedRejections;
        }

        public AtomicLong getPrimaryRejections() {
            return primaryRejections;
        }

        public AtomicLong getPrimaryNodeLimitsBreachedRejections() {
            return primaryNodeLimitsBreachedRejections;
        }

        public AtomicLong getPrimaryLastSuccessfulRequestLimitsBreachedRejections() {
            return primaryLastSuccessfulRequestLimitsBreachedRejections;
        }

        public AtomicLong getPrimaryThroughputDegradationLimitsBreachedRejections() {
            return primaryThroughputDegradationLimitsBreachedRejections;
        }

        public AtomicLong getReplicaRejections() {
            return replicaRejections;
        }

        public AtomicLong getReplicaNodeLimitsBreachedRejections() {
            return replicaNodeLimitsBreachedRejections;
        }

        public AtomicLong getReplicaLastSuccessfulRequestLimitsBreachedRejections() {
            return replicaLastSuccessfulRequestLimitsBreachedRejections;
        }

        public AtomicLong getReplicaThroughputDegradationLimitsBreachedRejections() {
            return replicaThroughputDegradationLimitsBreachedRejections;
        }
    }

    //Last Successful TimeStamp Tracker
    public static class TimeStampTracker {
        private final AtomicLong lastSuccessfulCoordinatingRequestTimestamp = new AtomicLong();
        private final AtomicLong lastSuccessfulPrimaryRequestTimestamp = new AtomicLong();
        private final AtomicLong lastSuccessfulReplicaRequestTimestamp = new AtomicLong();

        public AtomicLong getLastSuccessfulCoordinatingRequestTimestamp() {
            return lastSuccessfulCoordinatingRequestTimestamp;
        }

        public AtomicLong getLastSuccessfulPrimaryRequestTimestamp() {
            return lastSuccessfulPrimaryRequestTimestamp;
        }

        public AtomicLong getLastSuccessfulReplicaRequestTimestamp() {
            return lastSuccessfulReplicaRequestTimestamp;
        }
    }

    //Total Outstanding requests after last successful request
    public static class OutstandingRequestTracker {
        private final AtomicLong totalOutstandingCoordinatingRequests = new AtomicLong();
        private final AtomicLong totalOutstandingPrimaryRequests = new AtomicLong();
        private final AtomicLong totalOutstandingReplicaRequests = new AtomicLong();

        public AtomicLong getTotalOutstandingCoordinatingRequests() {
            return totalOutstandingCoordinatingRequests;
        }

        public AtomicLong getTotalOutstandingPrimaryRequests() {
            return totalOutstandingPrimaryRequests;
        }

        public AtomicLong getTotalOutstandingReplicaRequests() {
            return totalOutstandingReplicaRequests;
        }
    }

    // Throughput/Moving avg Tracker
    public static class ThroughputTracker {
        /*
        Shard Window Throughput Tracker.
        We will be using atomic long to track double values as mentioned here -
        https://docs.oracle.com/javase/6/docs/api/java/util/concurrent/atomic/package-summary.html
        */
        private final AtomicLong coordinatingThroughputMovingAverage = new AtomicLong();
        private final AtomicLong primaryThroughputMovingAverage = new AtomicLong();
        private final AtomicLong replicaThroughputMovingAverage = new AtomicLong();

        //Shard Window Throughput Queue
        private final ConcurrentLinkedQueue<Double> coordinatingThroughputMovingQueue = new ConcurrentLinkedQueue();
        private final ConcurrentLinkedQueue<Double> primaryThroughputMovingQueue = new ConcurrentLinkedQueue();
        private final ConcurrentLinkedQueue<Double> replicaThroughputMovingQueue = new ConcurrentLinkedQueue();

        public AtomicLong getCoordinatingThroughputMovingAverage() {
            return coordinatingThroughputMovingAverage;
        }

        public AtomicLong getPrimaryThroughputMovingAverage() {
            return primaryThroughputMovingAverage;
        }

        public AtomicLong getReplicaThroughputMovingAverage() {
            return replicaThroughputMovingAverage;
        }

        public ConcurrentLinkedQueue<Double> getCoordinatingThroughputMovingQueue() {
            return coordinatingThroughputMovingQueue;
        }

        public ConcurrentLinkedQueue<Double> getPrimaryThroughputMovingQueue() {
            return primaryThroughputMovingQueue;
        }

        public ConcurrentLinkedQueue<Double> getReplicaThroughputMovingQueue() {
            return replicaThroughputMovingQueue;
        }
    }
}
