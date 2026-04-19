package org.opensearch.search.profile.query;

import java.util.Map;

/**
 * SegmentInformation is a data object that holds the information for a segment. It
 * gives profiling information on the internals of a single segment within a slice.
 */
public class SegmentInformation {
    static final String SEGMENT_ID = "segment_id";
    static final String FROM = "from";
    static final String TO = "to";

    private final int segmentId;
    private final int from;
    private final int to;

    public SegmentInformation(int segmentId, int from, int to) {
        this.segmentId = segmentId;
        this.from = from;
        // The to value is exclusive, which means the actual doc count in this segment is to - from.
        this.to = to;
    }

    int getDocCount() {
        return to - from;
    }

    /**
     * Convert the segment information to a map for serialization and user readability.
     * @return
     */
    public Map<String, Object> toMap() {
        return Map.of(SEGMENT_ID, segmentId, FROM, from, TO, to);
    }
}
