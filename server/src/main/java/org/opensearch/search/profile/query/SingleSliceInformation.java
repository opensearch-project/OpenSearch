package org.opensearch.search.profile.query;

import java.util.List;
import java.util.Map;

public class SingleSliceInformation {
    static final String PARTITION_COUNT = "partition_count";
    static final String DOC_COUNT = "doc_count";
    static final String SEGMENTS = "segments";

    private final int partitionCount;
    private final int docCount;
    private final List<SegmentInformation> segments;

    public SingleSliceInformation(int partitionCount, int docCount, List<SegmentInformation> segments) {
        this.partitionCount = partitionCount;
        this.docCount = docCount;
        this.segments = List.of();
    }

    public Map<String, Object> toMap() {
        return Map.of(PARTITION_COUNT, partitionCount, DOC_COUNT, docCount, SEGMENTS, segments);
    }
}
