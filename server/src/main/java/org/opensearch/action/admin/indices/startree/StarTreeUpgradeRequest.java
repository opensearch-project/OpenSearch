/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.startree;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ValidateActions;
import org.opensearch.action.support.broadcast.BroadcastRequest;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.index.compositeindex.datacube.DimensionFactory;
import org.opensearch.index.compositeindex.datacube.Metric;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeFieldConfiguration;
import org.opensearch.index.mapper.CompositeDataCubeFieldType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * A request to upgrade an existing index by adding star tree configuration.
 * The star tree field configuration is parsed from the request body and
 * serialized as XContent bytes for transport across nodes.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class StarTreeUpgradeRequest extends BroadcastRequest<StarTreeUpgradeRequest> {

    private static final String STAR_TREE = "star_tree";
    private static final String ORDERED_DIMENSIONS = "ordered_dimensions";
    private static final String METRICS = "metrics";
    private static final String STATS = "stats";
    private static final String MAX_LEAF_DOCS = "max_leaf_docs";
    private static final String SKIP_STAR_NODE_IN_DIMS = "skip_star_node_creation_for_dimensions";
    private static final int DEFAULT_MAX_LEAF_DOCS = 10000;

    private final StarTreeField starTreeField;

    /**
     * Constructs a star tree upgrade request for the given indices with the specified star tree field configuration.
     *
     * @param indices the target indices to upgrade
     * @param starTreeField the star tree field configuration
     */
    public StarTreeUpgradeRequest(String[] indices, StarTreeField starTreeField) {
        super(indices);
        this.starTreeField = starTreeField;
    }

    /**
     * Constructs a star tree upgrade request from a stream input for transport deserialization.
     *
     * @param in the stream input to read from
     * @throws IOException if an I/O error occurs
     */
    public StarTreeUpgradeRequest(StreamInput in) throws IOException {
        super(in);
        BytesReference source = in.readBytesReference();
        this.starTreeField = parseStarTreeFieldFromBytes(source);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        BytesReference source = serializeStarTreeFieldToBytes(starTreeField);
        out.writeBytesReference(source);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (starTreeField == null) {
            validationException = ValidateActions.addValidationError("star tree field configuration is required", validationException);
        }
        if (indices == null || indices.length == 0) {
            validationException = ValidateActions.addValidationError("index is required", validationException);
        }
        return validationException;
    }

    /**
     * Returns the star tree field configuration for this upgrade request.
     */
    public StarTreeField getStarTreeField() {
        return starTreeField;
    }

    /**
     * Parses a star tree field configuration from an XContent request body.
     * Expected format:
     * <pre>
     * {
     *   "star_tree": {
     *     "name": "my_star_tree",
     *     "ordered_dimensions": [ { "name": "field1" }, ... ],
     *     "metrics": [ { "name": "field2", "stats": ["sum", "avg"] }, ... ],
     *     "max_leaf_docs": 10000,
     *     "skip_star_node_creation_for_dimensions": ["field1"]
     *   }
     * }
     * </pre>
     *
     * @param content the request body bytes
     * @param mediaType the media type of the content
     * @return the parsed StarTreeField
     * @throws IOException if parsing fails
     */
    @SuppressWarnings("unchecked")
    public static StarTreeField parseStarTreeConfig(BytesReference content, MediaType mediaType) throws IOException {
        Map<String, Object> bodyMap = XContentHelper.convertToMap(content, false, mediaType).v2();
        Object starTreeObj = bodyMap.get(STAR_TREE);
        if (starTreeObj == null) {
            throw new IllegalArgumentException("request body must contain a [star_tree] configuration");
        }
        if (starTreeObj instanceof Map == false) {
            throw new IllegalArgumentException("[star_tree] must be an object");
        }
        Map<String, Object> starTreeMap = (Map<String, Object>) starTreeObj;
        return parseStarTreeFieldFromMap(starTreeMap);
    }

    private static StarTreeField parseStarTreeFieldFromMap(Map<String, Object> starTreeMap) {
        String name = (String) starTreeMap.get(CompositeDataCubeFieldType.NAME);
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("[name] is required for star tree configuration");
        }

        // Remove type field if present (it's informational)
        starTreeMap.remove("type");

        // Parse max_leaf_docs
        int maxLeafDocs = DEFAULT_MAX_LEAF_DOCS;
        if (starTreeMap.containsKey(MAX_LEAF_DOCS)) {
            maxLeafDocs = ((Number) starTreeMap.get(MAX_LEAF_DOCS)).intValue();
            if (maxLeafDocs < 1) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "%s [%s] must be greater than 0", MAX_LEAF_DOCS, maxLeafDocs)
                );
            }
        }

        // Parse skip_star_node_creation_for_dimensions
        Set<String> skipStarInDims = new LinkedHashSet<>();
        if (starTreeMap.containsKey(SKIP_STAR_NODE_IN_DIMS)) {
            Object skipDimsObj = starTreeMap.get(SKIP_STAR_NODE_IN_DIMS);
            if (skipDimsObj instanceof List) {
                for (Object dim : (List<?>) skipDimsObj) {
                    skipStarInDims.add(dim.toString());
                }
            }
        }

        // Parse dimensions
        List<Dimension> dimensions = parseDimensions(name, starTreeMap);

        // Parse metrics
        List<Metric> metrics = parseMetrics(name, starTreeMap);

        // Validate skip dims against dimensions
        for (String dim : skipStarInDims) {
            if (dimensions.stream().filter(d -> d.getField().equals(dim)).findAny().isEmpty()) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "[%s] in skip_star_node_creation_for_dimensions should be part of ordered_dimensions", dim)
                );
            }
        }

        StarTreeFieldConfiguration config = new StarTreeFieldConfiguration(
            maxLeafDocs,
            skipStarInDims,
            StarTreeFieldConfiguration.StarTreeBuildMode.OFF_HEAP
        );

        return new StarTreeField(name, dimensions, metrics, config);
    }

    @SuppressWarnings("unchecked")
    private static List<Dimension> parseDimensions(String fieldName, Map<String, Object> map) {
        Object dims = map.get(ORDERED_DIMENSIONS);
        if (dims == null) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "ordered_dimensions is required for star tree field [%s]", fieldName)
            );
        }
        if (dims instanceof List == false) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "unable to parse ordered_dimensions for star tree field [%s]", fieldName)
            );
        }
        List<Object> dimsList = (List<Object>) dims;
        if (dimsList.size() < 2) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "At least two dimensions are required to build star tree index field [%s]", fieldName)
            );
        }
        List<Dimension> dimensions = new LinkedList<>();
        Set<String> dimensionFieldNames = new LinkedHashSet<>();
        for (Object dim : dimsList) {
            if (dim instanceof Map == false) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "unable to parse ordered_dimensions for star tree field [%s]", fieldName)
                );
            }
            Map<String, Object> dimMap = (Map<String, Object>) dim;
            String dimName = (String) dimMap.get(CompositeDataCubeFieldType.NAME);
            if (dimName == null || dimName.isEmpty()) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "dimension [name] is required for star tree field [%s]", fieldName)
                );
            }
            String type = (String) dimMap.get(CompositeDataCubeFieldType.TYPE);
            if (type == null) {
                // Default to numeric type when no type is specified in the upgrade request
                type = "numeric";
            }
            if (dimensionFieldNames.add(dimName) == false) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "Duplicate dimension [%s] present as part star tree index field [%s]", dimName, fieldName)
                );
            }
            dimensions.add(DimensionFactory.parseAndCreateDimension(dimName, type, new java.util.HashMap<>(dimMap), null));
        }
        return dimensions;
    }

    @SuppressWarnings("unchecked")
    private static List<Metric> parseMetrics(String fieldName, Map<String, Object> map) {
        Object metricsObj = map.get(METRICS);
        if (metricsObj == null) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "metrics section is required for star tree field [%s]", fieldName)
            );
        }
        if (metricsObj instanceof List == false) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "unable to parse metrics for star tree field [%s]", fieldName));
        }
        List<?> metricsList = (List<?>) metricsObj;
        List<Metric> metrics = new LinkedList<>();
        Set<String> metricFieldNames = new LinkedHashSet<>();
        for (Object metricObj : metricsList) {
            if (metricObj instanceof Map == false) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "unable to parse metrics for star tree field [%s]", fieldName)
                );
            }
            Map<String, Object> metricMap = (Map<String, Object>) metricObj;
            String metricName = (String) metricMap.get(CompositeDataCubeFieldType.NAME);
            if (metricName == null || metricName.isEmpty()) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "metric [name] is required for star tree field [%s]", fieldName)
                );
            }
            if (metricFieldNames.add(metricName) == false) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "Duplicate metrics [%s] present as part star tree index field [%s]", metricName, fieldName)
                );
            }
            List<MetricStat> metricStats = parseMetricStats(metricMap);
            metrics.add(new Metric(metricName, metricStats));
        }
        return metrics;
    }

    private static List<MetricStat> parseMetricStats(Map<String, Object> metricMap) {
        Object statsObj = metricMap.get(STATS);
        Set<MetricStat> metricSet = new LinkedHashSet<>();
        if (statsObj instanceof List) {
            List<?> statsList = (List<?>) statsObj;
            for (Object stat : statsList) {
                MetricStat metricStat = MetricStat.fromTypeName(stat.toString());
                metricSet.add(metricStat);
                addBaseMetrics(metricStat, metricSet);
            }
        }
        if (metricSet.isEmpty()) {
            // Default metrics: value_count and sum (matching StarTreeIndexSettings.DEFAULT_METRICS_LIST)
            metricSet.add(MetricStat.VALUE_COUNT);
            metricSet.add(MetricStat.SUM);
        }
        addEligibleDerivedMetrics(metricSet);
        return new ArrayList<>(metricSet);
    }

    private static void addBaseMetrics(MetricStat metricStat, Set<MetricStat> metricSet) {
        if (metricStat.isDerivedMetric()) {
            java.util.Queue<MetricStat> metricQueue = new LinkedList<>(metricStat.getBaseMetrics());
            while (metricQueue.isEmpty() == false) {
                MetricStat metric = metricQueue.poll();
                if (metric.isDerivedMetric() && metricSet.contains(metric) == false) {
                    metricQueue.addAll(metric.getBaseMetrics());
                }
                metricSet.add(metric);
            }
        }
    }

    private static void addEligibleDerivedMetrics(Set<MetricStat> metricStats) {
        for (MetricStat metric : MetricStat.values()) {
            if (metric.isDerivedMetric() && metricStats.contains(metric) == false) {
                List<MetricStat> sourceMetrics = metric.getBaseMetrics();
                if (metricStats.containsAll(sourceMetrics)) {
                    metricStats.add(metric);
                }
            }
        }
    }

    private static BytesReference serializeStarTreeFieldToBytes(StarTreeField starTreeField) throws IOException {
        try (XContentBuilder builder = org.opensearch.common.xcontent.XContentFactory.jsonBuilder()) {
            starTreeField.toXContent(builder, ToXContent.EMPTY_PARAMS);
            return BytesReference.bytes(builder);
        }
    }

    private static StarTreeField parseStarTreeFieldFromBytes(BytesReference bytes) throws IOException {
        Map<String, Object> map = XContentHelper.convertToMap(bytes, false, MediaTypeRegistry.JSON).v2();
        return parseStarTreeFieldFromMap(map);
    }

    @Override
    public String toString() {
        return "StarTreeUpgradeRequest{" + "starTreeField=" + (starTreeField != null ? starTreeField.getName() : "null") + '}';
    }
}
