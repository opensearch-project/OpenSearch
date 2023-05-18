/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.correlation.rules.model;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.core.ParseField;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Correlation Query DSL
 * {
 *   "index": "s3_access_logs",
 *   "query": "aws.cloudtrail.eventName:ReplicateObject",
 *   "timestampField": "@timestamp",
 *   "tags": [
 *     "s3"
 *   ]
 * }
 */
public class CorrelationQuery implements Writeable, ToXContentObject {

    private static final Logger log = LogManager.getLogger(CorrelationQuery.class);
    private static final ParseField INDEX_FIELD = new ParseField("index");
    private static final ParseField QUERY_FIELD = new ParseField("query");
    private static final ParseField TIMESTAMP_FIELD = new ParseField("timestampField");
    private static final ParseField TAGS_FIELD = new ParseField("tags");
    private static final ObjectParser<CorrelationQuery, Void> PARSER = new ObjectParser<>("CorrelationQuery", CorrelationQuery::new);

    static {
        PARSER.declareString(CorrelationQuery::setIndex, INDEX_FIELD);
        PARSER.declareString(CorrelationQuery::setQuery, QUERY_FIELD);
        PARSER.declareStringOrNull(CorrelationQuery::setTimestampField, TIMESTAMP_FIELD);
        PARSER.declareField((xcp, query, context) -> {
            List<String> tags = new ArrayList<>();
            XContentParser.Token currentToken = xcp.currentToken();
            if (currentToken == XContentParser.Token.START_ARRAY) {
                while (xcp.nextToken() != XContentParser.Token.END_ARRAY) {
                    tags.add(xcp.text());
                }
            }
            query.setTags(tags);
        }, TAGS_FIELD, ObjectParser.ValueType.STRING_ARRAY);
    }

    private String index;

    private String query;

    private String timestampField;

    private List<String> tags;

    private CorrelationQuery() {
        this.timestampField = "_timestamp";
    }

    /**
     * Parameterized ctor of Correlation Query
     * @param index event index to correlate
     * @param query query to filter relevant events for correlations from index
     * @param timestampField timestamp field in the index
     * @param tags tags to store additional metadata as part of correlation queries.
     */
    public CorrelationQuery(String index, String query, String timestampField, List<String> tags) {
        this.index = index;
        this.query = query;
        this.timestampField = timestampField != null ? timestampField : "_timestamp";
        this.tags = tags;
    }

    /**
     * StreamInput ctor of Correlation Query
     * @param sin StreamInput
     * @throws IOException IOException
     */
    public CorrelationQuery(StreamInput sin) throws IOException {
        this(sin.readString(), sin.readString(), sin.readString(), sin.readStringList());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(index);
        out.writeString(query);
        out.writeString(timestampField);
        out.writeStringCollection(tags);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(INDEX_FIELD.getPreferredName(), index)
            .field(QUERY_FIELD.getPreferredName(), query)
            .field(TIMESTAMP_FIELD.getPreferredName(), timestampField)
            .field(TAGS_FIELD.getPreferredName(), tags);
        return builder.endObject();
    }

    /**
     * parse into CorrelationQuery
     * @param xcp XContentParser
     * @return CorrelationQuery
     */
    public static CorrelationQuery parse(XContentParser xcp) {
        return PARSER.apply(xcp, null);
    }

    /**
     * convert StreamInput to CorrelationQuery
     * @param sin StreamInput
     * @return CorrelationQuery
     * @throws IOException IOException
     */
    public static CorrelationQuery readFrom(StreamInput sin) throws IOException {
        return new CorrelationQuery(sin);
    }

    /**
     * Set index
     * @param index event index to correlate
     */
    public void setIndex(String index) {
        this.index = index;
    }

    /**
     * Get index
     * @return event index to correlate
     */
    public String getIndex() {
        return index;
    }

    /**
     * Set query
     * @param query query to filter relevant events for correlations from index
     */
    public void setQuery(String query) {
        this.query = query;
    }

    /**
     * Get query
     * @return query to filter relevant events for correlations from index
     */
    public String getQuery() {
        return query;
    }

    /**
     * Set timestamp field
     * @param timestampField timestamp field in the index
     */
    public void setTimestampField(String timestampField) {
        this.timestampField = timestampField != null ? timestampField : "_timestamp";
    }

    /**
     * Get timestamp field
     * @return timestamp field in the index
     */
    public String getTimestampField() {
        return timestampField;
    }

    /**
     * Set tags
     * @param tags tags to store additional metadata as part of correlation queries.
     */
    public void setTags(List<String> tags) {
        this.tags = tags;
    }

    /**
     * Get tags
     * @return tags to store additional metadata as part of correlation queries.
     */
    public List<String> getTags() {
        return tags;
    }
}
