package org.opensearch.search.profile.query;

import java.util.List;
import java.util.Map;

public class SlicesInformation {
    static final String SLICE_COUNT = "slice_count";
    static final String SLICES = "slices";

    private final List<SingleSliceInformation> slices;

    public SlicesInformation() {
        this.slices = List.of();
    }

    public Map<String, Object> toMap() {
        return Map.of(SLICE_COUNT, slices.size(), SLICES, slices);
    }
}
