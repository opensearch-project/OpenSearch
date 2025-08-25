/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.builder;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * Streaming search parameters for configuring progressive result emission.
 * These parameters control how and when intermediate results are sent.
 */
public class StreamingSearchParameters implements Writeable, ToXContent {
    
    public static final String STREAMING_FIELD = "streaming";
    public static final String ENABLED_FIELD = "enabled";
    public static final String CONFIDENCE_FIELD = "confidence";
    public static final String BATCH_SIZE_FIELD = "batch_size";
    public static final String EMISSION_INTERVAL_FIELD = "emission_interval";
    public static final String MIN_DOCS_FIELD = "min_docs";
    public static final String ADAPTIVE_BATCHING_FIELD = "adaptive_batching";
    public static final String MILESTONES_FIELD = "milestones";
    
    private boolean enabled = false;
    private float initialConfidence = 0.99f;
    private int batchSize = 10;
    private int emissionIntervalMillis = 100;
    private int minDocsForStreaming = 5;
    private boolean adaptiveBatching = true;
    private boolean useMilestones = true;
    
    public StreamingSearchParameters() {}
    
    public StreamingSearchParameters(StreamInput in) throws IOException {
        this.enabled = in.readBoolean();
        this.initialConfidence = in.readFloat();
        this.batchSize = in.readVInt();
        this.emissionIntervalMillis = in.readVInt();
        this.minDocsForStreaming = in.readVInt();
        this.adaptiveBatching = in.readBoolean();
        this.useMilestones = in.readBoolean();
    }
    
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(enabled);
        out.writeFloat(initialConfidence);
        out.writeVInt(batchSize);
        out.writeVInt(emissionIntervalMillis);
        out.writeVInt(minDocsForStreaming);
        out.writeBoolean(adaptiveBatching);
        out.writeBoolean(useMilestones);
    }
    
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(STREAMING_FIELD);
        builder.field(ENABLED_FIELD, enabled);
        builder.field(CONFIDENCE_FIELD, initialConfidence);
        builder.field(BATCH_SIZE_FIELD, batchSize);
        builder.field(EMISSION_INTERVAL_FIELD, emissionIntervalMillis);
        builder.field(MIN_DOCS_FIELD, minDocsForStreaming);
        builder.field(ADAPTIVE_BATCHING_FIELD, adaptiveBatching);
        builder.field(MILESTONES_FIELD, useMilestones);
        builder.endObject();
        return builder;
    }
    
    public static StreamingSearchParameters fromXContent(XContentParser parser) throws IOException {
        StreamingSearchParameters params = new StreamingSearchParameters();
        
        XContentParser.Token token;
        String currentFieldName = null;
        
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (ENABLED_FIELD.equals(currentFieldName)) {
                    params.enabled = parser.booleanValue();
                } else if (CONFIDENCE_FIELD.equals(currentFieldName)) {
                    params.initialConfidence = parser.floatValue();
                } else if (BATCH_SIZE_FIELD.equals(currentFieldName)) {
                    params.batchSize = parser.intValue();
                } else if (EMISSION_INTERVAL_FIELD.equals(currentFieldName)) {
                    params.emissionIntervalMillis = parser.intValue();
                } else if (MIN_DOCS_FIELD.equals(currentFieldName)) {
                    params.minDocsForStreaming = parser.intValue();
                } else if (ADAPTIVE_BATCHING_FIELD.equals(currentFieldName)) {
                    params.adaptiveBatching = parser.booleanValue();
                } else if (MILESTONES_FIELD.equals(currentFieldName)) {
                    params.useMilestones = parser.booleanValue();
                }
            }
        }
        
        return params;
    }
    
    // Getters and setters
    
    public boolean isEnabled() {
        return enabled;
    }
    
    public StreamingSearchParameters enabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }
    
    public float getInitialConfidence() {
        return initialConfidence;
    }
    
    public StreamingSearchParameters initialConfidence(float confidence) {
        if (confidence <= 0.0f || confidence > 1.0f) {
            throw new IllegalArgumentException("Confidence must be between 0 and 1");
        }
        this.initialConfidence = confidence;
        return this;
    }
    
    public int getBatchSize() {
        return batchSize;
    }
    
    public StreamingSearchParameters batchSize(int batchSize) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("Batch size must be positive");
        }
        this.batchSize = batchSize;
        return this;
    }
    
    public int getEmissionIntervalMillis() {
        return emissionIntervalMillis;
    }
    
    public StreamingSearchParameters emissionIntervalMillis(int interval) {
        if (interval < 0) {
            throw new IllegalArgumentException("Emission interval cannot be negative");
        }
        this.emissionIntervalMillis = interval;
        return this;
    }
    
    public int getMinDocsForStreaming() {
        return minDocsForStreaming;
    }
    
    public StreamingSearchParameters minDocsForStreaming(int minDocs) {
        if (minDocs < 0) {
            throw new IllegalArgumentException("Min docs cannot be negative");
        }
        this.minDocsForStreaming = minDocs;
        return this;
    }
    
    public boolean isAdaptiveBatching() {
        return adaptiveBatching;
    }
    
    public StreamingSearchParameters adaptiveBatching(boolean adaptive) {
        this.adaptiveBatching = adaptive;
        return this;
    }
    
    public boolean isUseMilestones() {
        return useMilestones;
    }
    
    public StreamingSearchParameters useMilestones(boolean useMilestones) {
        this.useMilestones = useMilestones;
        return this;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StreamingSearchParameters that = (StreamingSearchParameters) o;
        return enabled == that.enabled &&
               Float.compare(that.initialConfidence, initialConfidence) == 0 &&
               batchSize == that.batchSize &&
               emissionIntervalMillis == that.emissionIntervalMillis &&
               minDocsForStreaming == that.minDocsForStreaming &&
               adaptiveBatching == that.adaptiveBatching &&
               useMilestones == that.useMilestones;
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(enabled, initialConfidence, batchSize, 
                          emissionIntervalMillis, minDocsForStreaming, 
                          adaptiveBatching, useMilestones);
    }
    
    @Override
    public String toString() {
        return "StreamingSearchParameters{" +
               "enabled=" + enabled +
               ", confidence=" + initialConfidence +
               ", batchSize=" + batchSize +
               ", emissionInterval=" + emissionIntervalMillis +
               ", minDocs=" + minDocsForStreaming +
               ", adaptive=" + adaptiveBatching +
               ", milestones=" + useMilestones +
               '}';
    }
}