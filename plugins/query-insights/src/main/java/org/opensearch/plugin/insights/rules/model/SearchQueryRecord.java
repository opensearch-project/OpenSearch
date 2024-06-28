/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.model;

import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * SearchQueryRecord represents a minimal atomic record stored in the Query Insight Framework,
 * which contains extensive information related to a search query.
 *
 * @opensearch.internal
 */
public class SearchQueryRecord implements ToXContentObject, Writeable {
    private final long timestamp;
    private final Map<MetricType, Number> measurements;
    private final Map<Attribute, Object> attributes;

    /**
     * Constructor of SearchQueryRecord
     *
     * @param in the StreamInput to read the SearchQueryRecord from
     * @throws IOException IOException
     * @throws ClassCastException ClassCastException
     */
    public SearchQueryRecord(final StreamInput in) throws IOException, ClassCastException {
        this.timestamp = in.readLong();
        measurements = new HashMap<>();
        in.readMap(MetricType::readFromStream, StreamInput::readGenericValue)
            .forEach(((metricType, o) -> measurements.put(metricType, metricType.parseValue(o))));
        this.attributes = in.readMap(Attribute::readFromStream, StreamInput::readGenericValue);
    }

    /**
     * Constructor of SearchQueryRecord
     *
     * @param timestamp The timestamp of the query.
     * @param measurements A list of Measurement associated with this query
     * @param attributes A list of Attributes associated with this query
     */
    public SearchQueryRecord(final long timestamp, Map<MetricType, Number> measurements, final Map<Attribute, Object> attributes) {
        if (measurements == null) {
            throw new IllegalArgumentException("Measurements cannot be null");
        }
        this.measurements = measurements;
        this.attributes = attributes;
        this.timestamp = timestamp;
    }

    /**
     * Returns the observation time of the metric.
     *
     * @return the observation time in milliseconds
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Returns the measurement associated with the specified name.
     *
     * @param name the name of the measurement
     * @return the measurement object, or null if not found
     */
    public Number getMeasurement(final MetricType name) {
        return measurements.get(name);
    }

    /**
     * Returns a map of all the measurements associated with the metric.
     *
     * @return a map of measurement names to measurement objects
     */
    public Map<MetricType, Number> getMeasurements() {
        return measurements;
    }

    /**
     * Returns a map of the attributes associated with the metric.
     *
     * @return a map of attribute keys to attribute values
     */
    public Map<Attribute, Object> getAttributes() {
        return attributes;
    }

    /**
     * Add an attribute to this record
     *
     * @param attribute attribute to add
     * @param value the value associated with the attribute
     */
    public void addAttribute(final Attribute attribute, final Object value) {
        attributes.put(attribute, value);
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field("timestamp", timestamp);
        for (Map.Entry<Attribute, Object> entry : attributes.entrySet()) {
            builder.field(entry.getKey().toString(), entry.getValue());
        }
        for (Map.Entry<MetricType, Number> entry : measurements.entrySet()) {
            builder.field(entry.getKey().toString(), entry.getValue());
        }
        return builder.endObject();
    }

    /**
     * Write a SearchQueryRecord to a StreamOutput
     *
     * @param out the StreamOutput to write
     * @throws IOException IOException
     */
    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeLong(timestamp);
        out.writeMap(measurements, (stream, metricType) -> MetricType.writeTo(out, metricType), StreamOutput::writeGenericValue);
        out.writeMap(attributes, (stream, attribute) -> Attribute.writeTo(out, attribute), StreamOutput::writeGenericValue);
    }

    /**
     * Compare two SearchQueryRecord, based on the given MetricType
     *
     * @param a the first SearchQueryRecord to compare
     * @param b the second SearchQueryRecord to compare
     * @param metricType the MetricType to compare on
     * @return 0 if the first SearchQueryRecord is numerically equal to the second SearchQueryRecord;
     *        -1 if the first SearchQueryRecord is numerically less than the second SearchQueryRecord;
     *         1 if the first SearchQueryRecord is numerically greater than the second SearchQueryRecord.
     */
    public static int compare(final SearchQueryRecord a, final SearchQueryRecord b, final MetricType metricType) {
        return metricType.compare(a.getMeasurement(metricType), b.getMeasurement(metricType));
    }

    /**
     * Check if a SearchQueryRecord is deep equal to another record
     *
     * @param o the other SearchQueryRecord record
     * @return true if two records are deep equal, false otherwise.
     */
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SearchQueryRecord)) {
            return false;
        }
        final SearchQueryRecord other = (SearchQueryRecord) o;
        return timestamp == other.getTimestamp()
            && measurements.equals(other.getMeasurements())
            && attributes.size() == other.getAttributes().size();
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, measurements, attributes);
    }

    @Override
    public String toString() {
        return Strings.toString(MediaTypeRegistry.JSON, this);
    }
}
