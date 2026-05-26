/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.action;

import org.opensearch.action.ActionType;

/**
 * Transport action for hash-shuffle worker fragments. Sibling of
 * {@link FragmentExecutionAction} but for worker-tier fragments that have no shard scan —
 * the worker fragment runs the consumer-side of a shuffle (e.g. the post-shuffle hash
 * join) and reads only from registered NamedScans backed by per-partition shuffle buffers.
 *
 * <p>Workers ride the same Arrow streaming response shape as shard fragments
 * ({@link FragmentExecutionArrowResponse}); only the request differs (no {@code shardId}).
 *
 * @opensearch.internal
 */
public class WorkerFragmentExecutionAction extends ActionType<FragmentExecutionArrowResponse> {

    public static final String NAME = "indices:data/read/analytics/worker_fragment";
    public static final WorkerFragmentExecutionAction INSTANCE = new WorkerFragmentExecutionAction();

    private WorkerFragmentExecutionAction() {
        super(NAME, FragmentExecutionArrowResponse::new);
    }
}
