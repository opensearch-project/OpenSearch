package org.opensearch.index.views;

import java.util.List;

import org.joda.time.DateTime;

public class ViewDefinition /* implements Writeable, ToXContentFragment */ {

    private String identifier;
    private String description;
    private DateTime createdAt;
    private DateTime modifiedAt;
    private String query; /** TBD format? */
    private List<IndexPattern> indexPatterns; 

    public static enum Computation {
        Materialized,
        Projected;
    }

    public static class IndexPattern {
        private String indexPattern;

        /** Define patterns properties */
    }
}
