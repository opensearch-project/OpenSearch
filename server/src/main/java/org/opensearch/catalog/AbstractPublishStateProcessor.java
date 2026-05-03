/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.catalog;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.threadpool.ThreadPool;

import java.util.Objects;

/**
 * Base class for phase processors. Each subclass drives transitions out of exactly one
 * {@link PublishPhase}. Retries use exponential backoff; exhaustion moves the entry to
 * {@link PublishPhase#FAILED}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public abstract class AbstractPublishStateProcessor {

    protected final Logger logger = LogManager.getLogger(getClass());

    protected final ClusterService clusterService;
    protected final CatalogMetadataClient metadataClient;
    protected final ThreadPool threadPool;
    private final int maxRetries;
    private final TimeValue baseBackoff;

    protected AbstractPublishStateProcessor(
        ClusterService clusterService,
        CatalogMetadataClient metadataClient,
        ThreadPool threadPool,
        int maxRetries,
        TimeValue baseBackoff
    ) {
        this.clusterService = Objects.requireNonNull(clusterService, "clusterService");
        this.metadataClient = Objects.requireNonNull(metadataClient, "metadataClient");
        this.threadPool = Objects.requireNonNull(threadPool, "threadPool");
        if (maxRetries < 0) {
            throw new IllegalArgumentException("maxRetries must be >= 0 but was [" + maxRetries + "]");
        }
        this.maxRetries = maxRetries;
        this.baseBackoff = Objects.requireNonNull(baseBackoff, "baseBackoff");
    }

    /** Invoked from the cluster-applier thread; implementations must offload blocking work. */
    public abstract void process(PublishEntry entry);

    protected abstract PublishPhase expectedPhase();

    /**
     * Submits an entry update, guarded by a re-check that the entry is still in
     * {@link #expectedPhase()} — stale updates are dropped silently.
     */
    protected final void submitEntryUpdate(String source, String publishId, java.util.function.UnaryOperator<PublishEntry> mutator) {
        clusterService.submitStateUpdateTask(source, new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                CatalogPublishesInProgress current = currentCustom(currentState);
                PublishEntry existing = current.entry(publishId);
                if (existing == null || existing.phase() != expectedPhase()) {
                    return currentState;
                }
                PublishEntry updated = mutator.apply(existing);
                if (updated == existing) {
                    return currentState;
                }
                return withCustom(currentState, current.withUpdatedEntry(updated));
            }

            @Override
            public void onFailure(String src, Exception e) {
                logger.warn("[publish-{}] cluster-state update [{}] failed", publishId, src, e);
            }
        });
    }

    /** Advances phase and resets retryCount. */
    protected final void moveToNextPhase(PublishEntry entry, PublishPhase next) {
        logger.info("[publish-{}] moving phase {} -> {}", entry.publishId(), entry.phase(), next);
        submitEntryUpdate(
            "catalog-publish-phase-change[" + entry.publishId() + "->" + next + "]",
            entry.publishId(),
            e -> PublishEntry.builder(e).phase(next).retryCount(0).build()
        );
    }

    protected final void removeEntry(PublishEntry entry) {
        logger.info("[publish-{}] removing completed entry", entry.publishId());
        String publishId = entry.publishId();
        clusterService.submitStateUpdateTask(
            "catalog-publish-remove[" + publishId + "]",
            new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    CatalogPublishesInProgress current = currentCustom(currentState);
                    if (current.entry(publishId) == null) {
                        return currentState;
                    }
                    return withCustom(currentState, current.withRemovedEntry(publishId));
                }

                @Override
                public void onFailure(String src, Exception e) {
                    logger.warn("[publish-{}] remove failed", publishId, e);
                }
            }
        );
    }

    /** Terminal transition. Entry stays so operators can read {@code lastFailureReason}. */
    protected final void moveToFailed(PublishEntry entry, String reason) {
        logger.warn("[publish-{}] moving to FAILED: {}", entry.publishId(), reason);
        submitEntryUpdate(
            "catalog-publish-failed[" + entry.publishId() + "]",
            entry.publishId(),
            e -> PublishEntry.builder(e).phase(PublishPhase.FAILED).lastFailureReason(reason).build()
        );
    }

    /** Bump retryCount and reschedule, or fall into {@link PublishPhase#FAILED} on exhaustion. */
    protected final void handleRetryOnError(PublishEntry entry, String reason, Throwable cause) {
        int next = entry.retryCount() + 1;
        if (next > maxRetries) {
            logger.error(
                "[publish-{}] retries exhausted in phase {} after {} attempts: {}",
                entry.publishId(), entry.phase(), entry.retryCount(), reason, cause
            );
            moveToFailed(entry, reason);
            return;
        }
        long backoffMillis = computeBackoffMillis(entry.retryCount());
        logger.warn(
            "[publish-{}] phase {} attempt {} failed ({}); retrying in {}ms",
            entry.publishId(), entry.phase(), entry.retryCount(), reason, backoffMillis, cause
        );

        submitEntryUpdate(
            "catalog-publish-retry[" + entry.publishId() + "#" + next + "]",
            entry.publishId(),
            PublishEntry::withIncrementedRetryCount
        );

        threadPool.schedule(
            () -> {
                PublishEntry latest = currentCustom(clusterService.state()).entry(entry.publishId());
                if (latest != null && latest.phase() == expectedPhase()) {
                    process(latest);
                }
            },
            TimeValue.timeValueMillis(backoffMillis),
            ThreadPool.Names.GENERIC
        );
    }

    /** {@code baseInterval × 2^retryCount}, clamped to Long-safe shift. */
    protected final long computeBackoffMillis(int retryCount) {
        long base = baseBackoff.getMillis();
        if (retryCount <= 0) return base;
        int shift = Math.min(retryCount, 62);
        return base << shift;
    }

    protected static CatalogPublishesInProgress currentCustom(ClusterState state) {
        CatalogPublishesInProgress c = state.metadata().custom(CatalogPublishesInProgress.TYPE);
        return c == null ? CatalogPublishesInProgress.EMPTY : c;
    }

    protected static ClusterState withCustom(ClusterState state, CatalogPublishesInProgress custom) {
        Metadata.Builder metaBuilder = Metadata.builder(state.metadata()).putCustom(CatalogPublishesInProgress.TYPE, custom);
        return ClusterState.builder(state).metadata(metaBuilder).build();
    }
}
