package org.opensearch.search.profile.query;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SlicesInformation {
    static final String SLICE_COUNT = "slice_count";
    static final String SLICES = "slices";

    private final List<SingleSliceInformation> slices;

    public SlicesInformation() {
        this.slices = new ArrayList<>();
    }

    public void addSlice(SingleSliceInformation singleSliceInformation) {
        this.slices.add(singleSliceInformation);
    }

    public List<SingleSliceInformation> getSlices() {
        return slices;
    }

    public Map<String, Object> toMap() {
        List<Map<String, Object>> slicesMap = new ArrayList<>();
        for (SingleSliceInformation slice : slices) {
            slicesMap.add(slice.toMap());
        }
        return Map.of(SLICE_COUNT, slices.size(), SLICES, slicesMap);
    }
}
