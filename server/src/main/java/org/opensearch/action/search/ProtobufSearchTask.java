/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.tasks.ProtobufCancellableTask;
import org.opensearch.tasks.SearchBackpressureTask;
import org.opensearch.tasks.ProtobufTaskId;

import java.util.Map;
import java.util.function.Supplier;

import static org.opensearch.search.SearchService.NO_TIMEOUT;

/**
 * Task storing information about a currently running {@link SearchRequest}.
 *
 * @opensearch.internal
 */
public class ProtobufSearchTask extends ProtobufCancellableTask implements SearchBackpressureTask {
    // generating description in a lazy way since source can be quite big
    private final Supplier<String> descriptionSupplier;
    private SearchProgressListener progressListener = SearchProgressListener.NOOP;

    public ProtobufSearchTask(
        long id,
        String type,
        String action,
        Supplier<String> descriptionSupplier,
        ProtobufTaskId parentTaskId,
        Map<String, String> headers
    ) {
        this(id, type, action, descriptionSupplier, parentTaskId, headers, NO_TIMEOUT);
    }

    public ProtobufSearchTask(
        long id,
        String type,
        String action,
        Supplier<String> descriptionSupplier,
        ProtobufTaskId parentTaskId,
        Map<String, String> headers,
        TimeValue cancelAfterTimeInterval
    ) {
        super(id, type, action, null, parentTaskId, headers, cancelAfterTimeInterval);
        this.descriptionSupplier = descriptionSupplier;
    }

    @Override
    public final String getDescription() {
        return descriptionSupplier.get();
    }

    @Override
    public boolean supportsResourceTracking() {
        return true;
    }

    /**
     * Attach a {@link SearchProgressListener} to this task.
     */
    public final void setProgressListener(SearchProgressListener progressListener) {
        this.progressListener = progressListener;
    }

    /**
     * Return the {@link SearchProgressListener} attached to this task.
     */
    public final SearchProgressListener getProgressListener() {
        return progressListener;
    }

    @Override
    public boolean shouldCancelChildrenOnCancellation() {
        return true;
    }
}
