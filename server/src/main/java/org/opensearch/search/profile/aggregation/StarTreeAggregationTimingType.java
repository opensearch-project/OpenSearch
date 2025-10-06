package org.opensearch.search.profile.aggregation;

import java.util.Locale;

public enum StarTreeAggregationTimingType {
    SCAN_STAR_TREE_SEGMENTS,
    BUILD_BUCKETS_FROM_STAR_TREE;

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }
}
