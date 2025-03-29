package org.opensearch.tasks;

import org.opensearch.core.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * Holds monitoring service stats specific to search task.
 */
public class SearchTaskCancellationStats extends BaseSearchTaskCancellationStats {

    public SearchTaskCancellationStats(long currentTaskCount, long totalTaskCount) {
        super(currentTaskCount, totalTaskCount);
    }

    public SearchTaskCancellationStats(StreamInput in) throws IOException {
        super(in);
    }
}
