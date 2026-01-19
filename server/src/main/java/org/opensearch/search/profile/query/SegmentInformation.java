package org.opensearch.search.profile.query;

import java.util.Map;

public class SegmentInformation {
    static final String SEGMENT_ID = "segment_id";
    static final String FROM = "from";
    static final String TO = "to";

    private final String segmentId;
    private final int from;
    private final int to;

    public SegmentInformation(String segmentId, int from, int to) {
        this.segmentId = segmentId;
        this.from = from;
        this.to = to;
    }

    public Map<String, Object> toMap() {
        return Map.of(SEGMENT_ID, segmentId, FROM, from, TO, to);
    }
}
