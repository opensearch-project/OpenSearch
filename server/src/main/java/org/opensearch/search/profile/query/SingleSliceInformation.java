package org.opensearch.search.profile.query;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SingleSliceInformation {
    static final String PARTITION_COUNT = "partition_count";
    static final String DOC_COUNT = "doc_count";
    static final String SEGMENTS = "segments";

    private int partitionCount;
    private int docCount;
    private final List<SegmentInformation> segments;

    public SingleSliceInformation() {
        this.docCount = 0;
        this.segments = new ArrayList<>();
    }

    public void addSegment(SegmentInformation segmentInformation) {
        this.segments.add(segmentInformation);
        this.docCount += segmentInformation.getDocCount();
        this.partitionCount++;
    }

    public Map<String, Object> toMap() {
        List<Map<String, Object>> segmentsMap = new ArrayList<>();
        for (SegmentInformation segmentInfo : segments) {
            segmentsMap.add(segmentInfo.toMap());
        }
        return Map.of(PARTITION_COUNT, partitionCount, DOC_COUNT, docCount, SEGMENTS, segmentsMap);
    }
}
