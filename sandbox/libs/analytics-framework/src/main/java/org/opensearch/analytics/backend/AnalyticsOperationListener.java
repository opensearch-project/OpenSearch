/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.backend;

import java.util.List;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

/**
 * Listener for analytics query execution lifecycle events. Analogous to
 * {@link org.opensearch.index.shard.SearchOperationListener} but designed
 * for the stage-based analytics execution model.
 *
 * <p>Covers two scopes:
 * <ul>
 *   <li><b>Coordinator-side:</b> stage lifecycle events (start, success,
 *       failure, cancellation) fired by the stage execution layer.</li>
 *   <li><b>Data-node-side:</b> fragment execution events (pre-execute,
 *       success, failure) fired by {@code AnalyticsSearchService}.</li>
 * </ul>
 *
 * <p>All methods have empty default implementations so listeners can
 * override only the events they care about.
 *
 * @opensearch.internal
 */
public interface AnalyticsOperationListener {

    // ─── Coordinator-side: query lifecycle ──────────────────────────────

    /**
     * Called when a new analytics query starts execution on the coordinator.
     *
     * @param queryId the unique query identifier
     * @param stageCount the number of stages in the execution graph
     */
    default void onQueryStart(String queryId, int stageCount) {}

    /**
     * Called when an analytics query completes successfully.
     *
     * @param queryId the unique query identifier
     * @param tookInNanos wall-clock time from query start to completion
     * @param totalRows total rows in the result set
     */
    default void onQuerySuccess(String queryId, long tookInNanos, long totalRows) {}

    /**
     * Called when an analytics query fails.
     *
     * @param queryId the unique query identifier
     * @param cause the exception that caused the failure
     */
    default void onQueryFailure(String queryId, Exception cause) {}

    // ─── Coordinator-side: stage lifecycle ──────────────────────────────

    /**
     * Called when a stage transitions to RUNNING.
     *
     * @param queryId the query this stage belongs to
     * @param stageId the stage identifier
     * @param stageType the stage execution class name
     */
    default void onStageStart(String queryId, int stageId, String stageType) {}

    /**
     * Called when a stage transitions to SUCCEEDED.
     *
     * @param queryId the query this stage belongs to
     * @param stageId the stage identifier
     * @param tookInNanos wall-clock time from stage start to completion
     * @param rowsProcessed number of rows processed by this stage
     */
    default void onStageSuccess(String queryId, int stageId, long tookInNanos, long rowsProcessed) {}

    /**
     * Called when a stage transitions to FAILED.
     *
     * @param queryId the query this stage belongs to
     * @param stageId the stage identifier
     * @param cause the exception that caused the failure
     */
    default void onStageFailure(String queryId, int stageId, Exception cause) {}

    /**
     * Called when a stage transitions to CANCELLED.
     *
     * @param queryId the query this stage belongs to
     * @param stageId the stage identifier
     * @param reason the cancellation reason
     */
    default void onStageCancelled(String queryId, int stageId, String reason) {}

    // ─── Data-node-side: fragment execution ─────────────────────────────

    /**
     * Called before a plan fragment is executed on a data node.
     *
     * @param queryId the query this fragment belongs to
     * @param stageId the stage this fragment belongs to
     * @param shardId the shard being queried
     */
    default void onPreFragmentExecution(String queryId, int stageId, String shardId) {}

    /**
     * Called after a plan fragment executes successfully on a data node.
     *
     * @param queryId the query this fragment belongs to
     * @param stageId the stage this fragment belongs to
     * @param shardId the shard that was queried
     * @param tookInNanos wall-clock time for the fragment execution
     * @param rowsProduced number of rows produced by this fragment
     */
    default void onFragmentSuccess(String queryId, int stageId, String shardId, long tookInNanos, long rowsProduced) {}

    /**
     * Called when a plan fragment fails on a data node.
     *
     * @param queryId the query this fragment belongs to
     * @param stageId the stage this fragment belongs to
     * @param shardId the shard that was queried
     * @param cause the exception that caused the failure
     */
    default void onFragmentFailure(String queryId, int stageId, String shardId, Exception cause) {}

    /**
     * Multiplexing composite that fans out each callback to a list of
     * delegates. Exception-safe — a failing delegate does not prevent
     * subsequent delegates from being notified.
     */
    class CompositeListener implements AnalyticsOperationListener {

        private final List<AnalyticsOperationListener> delegates;
        private static final Logger logger = LogManager.getLogger(CompositeListener.class);

        public CompositeListener(List<AnalyticsOperationListener> delegates) {
            this.delegates = delegates;
        }

        @Override public void onQueryStart(String queryId, int stageCount) {
            for (AnalyticsOperationListener l : delegates) {
                try { l.onQueryStart(queryId, stageCount); } catch (Exception e) { warn("onQueryStart", e); }
            }
        }

        @Override public void onQuerySuccess(String queryId, long tookInNanos, long totalRows) {
            for (AnalyticsOperationListener l : delegates) {
                try { l.onQuerySuccess(queryId, tookInNanos, totalRows); } catch (Exception e) { warn("onQuerySuccess", e); }
            }
        }

        @Override public void onQueryFailure(String queryId, Exception cause) {
            for (AnalyticsOperationListener l : delegates) {
                try { l.onQueryFailure(queryId, cause); } catch (Exception e) { warn("onQueryFailure", e); }
            }
        }

        @Override public void onStageStart(String queryId, int stageId, String stageType) {
            for (AnalyticsOperationListener l : delegates) {
                try { l.onStageStart(queryId, stageId, stageType); } catch (Exception e) { warn("onStageStart", e); }
            }
        }

        @Override public void onStageSuccess(String queryId, int stageId, long tookInNanos, long rowsProcessed) {
            for (AnalyticsOperationListener l : delegates) {
                try { l.onStageSuccess(queryId, stageId, tookInNanos, rowsProcessed); } catch (Exception e) { warn("onStageSuccess", e); }
            }
        }

        @Override public void onStageFailure(String queryId, int stageId, Exception cause) {
            for (AnalyticsOperationListener l : delegates) {
                try { l.onStageFailure(queryId, stageId, cause); } catch (Exception e) { warn("onStageFailure", e); }
            }
        }

        @Override public void onStageCancelled(String queryId, int stageId, String reason) {
            for (AnalyticsOperationListener l : delegates) {
                try { l.onStageCancelled(queryId, stageId, reason); } catch (Exception e) { warn("onStageCancelled", e); }
            }
        }

        @Override public void onPreFragmentExecution(String queryId, int stageId, String shardId) {
            for (AnalyticsOperationListener l : delegates) {
                try { l.onPreFragmentExecution(queryId, stageId, shardId); } catch (Exception e) { warn("onPreFragmentExecution", e); }
            }
        }

        @Override public void onFragmentSuccess(String queryId, int stageId, String shardId, long tookInNanos, long rowsProduced) {
            for (AnalyticsOperationListener l : delegates) {
                try { l.onFragmentSuccess(queryId, stageId, shardId, tookInNanos, rowsProduced); } catch (Exception e) { warn("onFragmentSuccess", e); }
            }
        }

        @Override public void onFragmentFailure(String queryId, int stageId, String shardId, Exception cause) {
            for (AnalyticsOperationListener l : delegates) {
                try { l.onFragmentFailure(queryId, stageId, shardId, cause); } catch (Exception e) { warn("onFragmentFailure", e); }
            }
        }

        private void warn(String method, Exception e) {
            logger.warn("[AnalyticsOperationListener.CompositeListener] {} threw", method, e);
        }
    }
}
