/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.correlation.events.model;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Correlation Object to store. A correlation is defined as an edge joining two events.
 *
 * @opensearch.internal
 */
public class Correlation implements Writeable, ToXContentObject {

    private static final Logger log = LogManager.getLogger(Correlation.class);

    /**
     * Correlation history index
     */
    public static final String CORRELATION_HISTORY_INDEX = ".opensearch-correlation-history";

    private static final ParseField ID_FIELD = new ParseField("id");
    private static final ParseField VERSION_FIELD = new ParseField("version");
    private static final ParseField ROOT_FIELD = new ParseField("root");
    private static final ParseField LEVEL_FIELD = new ParseField("level");
    private static final ParseField EVENT1_FIELD = new ParseField("event1");
    private static final ParseField EVENT2_FIELD = new ParseField("event2");
    private static final ParseField CORRELATION_VECTOR_FIELD = new ParseField("corr_vector");
    private static final ParseField TIMESTAMP_FIELD = new ParseField("timestamp");
    private static final ParseField INDEX1_FIELD = new ParseField("index1");
    private static final ParseField INDEX2_FIELD = new ParseField("index2");
    private static final ParseField TAGS_FIELD = new ParseField("tags");
    private static final ParseField SCORE_TIMESTAMP_FIELD = new ParseField("score_timestamp");
    private static final ObjectParser<Correlation, Void> PARSER = new ObjectParser<>("Correlation", Correlation::new);

    static {
        PARSER.declareBoolean(Correlation::setRoot, ROOT_FIELD);
        PARSER.declareString(Correlation::setId, ID_FIELD);
        PARSER.declareLong(Correlation::setVersion, VERSION_FIELD);
        PARSER.declareLong(Correlation::setLevel, LEVEL_FIELD);
        PARSER.declareString(Correlation::setEvent1, EVENT1_FIELD);
        PARSER.declareString(Correlation::setEvent2, EVENT2_FIELD);
        PARSER.declareFloatArray(Correlation::setCorrelationVector, CORRELATION_VECTOR_FIELD);
        PARSER.declareLong(Correlation::setTimestamp, TIMESTAMP_FIELD);
        PARSER.declareLong(Correlation::setScoreTimestamp, SCORE_TIMESTAMP_FIELD);
        PARSER.declareString(Correlation::setIndex1, INDEX1_FIELD);
        PARSER.declareString(Correlation::setIndex2, INDEX2_FIELD);
        PARSER.declareStringArray(Correlation::setTags, TAGS_FIELD);
    }

    private String id;

    private Long version;

    private Boolean isRoot;

    private Long level;

    private String event1;

    private String event2;

    private float[] correlationVector;

    private Long timestamp;

    private String index1;

    private String index2;

    private List<String> tags;

    private Long scoreTimestamp;

    private Correlation() {}

    /**
     * Parameterized ctor for Correlation
     * @param id id of correlation object
     * @param version version of correlation
     * @param isRoot is it root correlation record. The root record is used as the base record which store timestamp related metadata.
     * @param level level at which correlations are stored which help distinguish one group of correlations from another.
     * @param event1 first event which is correlated
     * @param event2 second event which is correlated
     * @param correlationVector the vector representation of the correlation object
     * @param timestamp timestamp of correlation
     * @param index1 index of the first event
     * @param index2 index of the second event
     * @param tags list of tags for the correlation object
     * @param scoreTimestamp score timestamp of correlation used to bypass the problem of Lucene vectors can only be of type byte array or floats.
     */
    public Correlation(
        String id,
        Long version,
        Boolean isRoot,
        Long level,
        String event1,
        String event2,
        float[] correlationVector,
        Long timestamp,
        String index1,
        String index2,
        List<String> tags,
        Long scoreTimestamp
    ) {
        this.id = id;
        this.version = version;
        this.isRoot = isRoot;
        this.level = level;
        this.event1 = event1;
        this.event2 = event2;
        this.correlationVector = correlationVector;
        this.timestamp = timestamp;
        this.index1 = index1;
        this.index2 = index2;
        this.tags = tags;
        this.scoreTimestamp = scoreTimestamp;
    }

    /**
     * Parameterized ctor of Correlation object
     * @param isRoot is it root correlation record. The root record is used as the base record which store timestamp related metadata.
     * @param level level at which correlations are stored which help distinguish one group of correlations from another.
     * @param event1 first event which is correlated
     * @param event2 second event which is correlated
     * @param correlationVector the vector representation of the correlation object
     * @param timestamp timestamp of correlation
     * @param index1 index of the first event
     * @param index2 index of the second event
     * @param tags list of tags for the correlation object
     * @param scoreTimestamp score timestamp of correlation used to bypass the problem of Lucene vectors can only be of type byte array or floats.
     */
    public Correlation(
        Boolean isRoot,
        Long level,
        String event1,
        String event2,
        float[] correlationVector,
        Long timestamp,
        String index1,
        String index2,
        List<String> tags,
        Long scoreTimestamp
    ) {
        this("", 1L, isRoot, level, event1, event2, correlationVector, timestamp, index1, index2, tags, scoreTimestamp);
    }

    /**
     * StreamInput ctor of Correlation object
     * @param sin StreamInput
     * @throws IOException IOException
     */
    public Correlation(StreamInput sin) throws IOException {
        this(
            sin.readString(),
            sin.readLong(),
            sin.readBoolean(),
            sin.readLong(),
            sin.readString(),
            sin.readString(),
            sin.readFloatArray(),
            sin.readLong(),
            sin.readString(),
            sin.readString(),
            sin.readStringList(),
            sin.readLong()
        );
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ROOT_FIELD.getPreferredName(), isRoot);
        builder.field(LEVEL_FIELD.getPreferredName(), level);
        builder.field(EVENT1_FIELD.getPreferredName(), event1);
        builder.field(EVENT2_FIELD.getPreferredName(), event2);
        builder.field(CORRELATION_VECTOR_FIELD.getPreferredName(), correlationVector);
        builder.field(TIMESTAMP_FIELD.getPreferredName(), timestamp);
        builder.field(INDEX1_FIELD.getPreferredName(), index1);
        builder.field(INDEX2_FIELD.getPreferredName(), index2);
        builder.field(TAGS_FIELD.getPreferredName(), tags);
        builder.field(SCORE_TIMESTAMP_FIELD.getPreferredName(), scoreTimestamp);
        return builder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeLong(version);
        out.writeBoolean(isRoot);
        out.writeLong(level);
        out.writeString(event1);
        out.writeString(event2);
        out.writeFloatArray(correlationVector);
        out.writeLong(timestamp);
        out.writeString(index1);
        out.writeString(index2);
        out.writeStringCollection(tags);
        out.writeLong(scoreTimestamp);
    }

    /**
     * Parse into Correlation
     * @param xcp XContentParser
     * @return Correlation
     * @throws IOException IOException
     */
    public static Correlation parse(XContentParser xcp) throws IOException {
        return PARSER.apply(xcp, null);
    }

    /**
     * convert StreamInput to Correlation
     * @param sin StreamInput
     * @return Correlation
     * @throws IOException IOException
     */
    public static Correlation readFrom(StreamInput sin) throws IOException {
        return new Correlation(sin);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Correlation that = (Correlation) o;
        return id.equals(that.id)
            && version.equals(that.version)
            && isRoot.equals(that.isRoot)
            && level.equals(that.level)
            && event1.equals(that.event1)
            && event2.equals(that.event2)
            && Arrays.equals(correlationVector, that.correlationVector)
            && timestamp.equals(that.timestamp)
            && index1.equals(that.index1)
            && index2.equals(that.index2)
            && tags.equals(that.tags)
            && scoreTimestamp.equals(that.scoreTimestamp);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(id, version, isRoot, level, event1, event2, timestamp, index1, index2, tags, scoreTimestamp);
        result = 31 * result + Arrays.hashCode(correlationVector);
        return result;
    }

    /**
     * set if it is root correlation record
     * @param root set if it is root correlation record
     */
    public void setRoot(Boolean root) {
        isRoot = root;
    }

    /**
     * set id of correlation object
     * @param id id of correlation object
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * set version of correlation object
     * @param version version of correlation object
     */
    public void setVersion(Long version) {
        this.version = version;
    }

    /**
     * set level of correlation object
     * @param level level of correlation object
     */
    public void setLevel(Long level) {
        this.level = level;
    }

    /**
     * set first event of correlation object
     * @param event1 first event of correlation object
     */
    public void setEvent1(String event1) {
        this.event1 = event1;
    }

    /**
     * set second event of correlation object
     * @param event2 second event of correlation object
     */
    public void setEvent2(String event2) {
        this.event2 = event2;
    }

    /**
     * set the vector representation of the correlation object
     * @param correlationVector the vector representation of the correlation object
     */
    public void setCorrelationVector(List<Float> correlationVector) {
        int size = correlationVector.size();
        this.correlationVector = new float[size];
        for (int i = 0; i < size; ++i) {
            this.correlationVector[i] = correlationVector.get(i);
        }
    }

    /**
     * set timestamp of correlation
     * @param timestamp timestamp of correlation
     */
    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * set score timestamp of correlation object
     * @param scoreTimestamp score timestamp of correlation object
     */
    public void setScoreTimestamp(Long scoreTimestamp) {
        this.scoreTimestamp = scoreTimestamp;
    }

    /**
     * set index of first event
     * @param index1 index of first event
     */
    public void setIndex1(String index1) {
        this.index1 = index1;
    }

    /**
     * set index of second event
     * @param index2 index of second event
     */
    public void setIndex2(String index2) {
        this.index2 = index2;
    }

    /**
     * set tags for correlation object
     * @param tags tags for correlation object
     */
    public void setTags(List<String> tags) {
        this.tags = tags;
    }
}
