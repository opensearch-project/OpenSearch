/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex;

import org.opensearch.common.Rounding;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.opensearch.index.compositeindex.CompositeIndexSettings.COMPOSITE_INDEX_ENABLED_SETTING;

/**
 * Configuration of composite index containing list of composite fields.
 * Each composite field contains dimensions, metrics along with composite index (eg: star tree) specific settings.
 * Each composite field will generate a composite index in indexing flow.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class CompositeIndexConfig {
    private static final Set<Class<? extends MappedFieldType>> ALLOWED_DIMENSION_MAPPED_FIELD_TYPES = Set.of(
        NumberFieldMapper.NumberFieldType.class,
        DateFieldMapper.DateFieldType.class
    );

    private static final Set<Class<? extends MappedFieldType>> ALLOWED_METRIC_MAPPED_FIELD_TYPES = Set.of(
        NumberFieldMapper.NumberFieldType.class
    );

    private static final String COMPOSITE_INDEX_CONFIG = "index.composite_index.config";
    private static final String DIMENSIONS_ORDER = "dimensions_order";
    private static final String DIMENSIONS_CONFIG = "dimensions_config";
    private static final String METRICS = "metrics";
    private static final String METRICS_CONFIG = "metrics_config";
    private static final String FIELD = "field";
    private static final String TYPE = "type";
    private static final String INDEX_MODE = "index_mode";
    private static final String STAR_TREE_BUILD_MODE = "build_mode";
    private static final String MAX_LEAF_DOCS = "max_leaf_docs";
    private static final String SKIP_STAR_NODE_CREATION_FOR_DIMS = "skip_star_node_creation_for_dimensions";
    private static final String SPEC = "_spec";
    private final List<CompositeField> compositeFields = new ArrayList<>();

    public CompositeIndexConfig(IndexSettings indexSettings) {

        final Map<String, Settings> compositeIndexSettings = indexSettings.getSettings().getGroups(COMPOSITE_INDEX_CONFIG);
        Set<String> fields = compositeIndexSettings.keySet();
        for (String field : fields) {
            compositeFields.add(buildCompositeField(field, compositeIndexSettings.get(field)));
        }
    }

    /**
     * This returns composite field after performing basic validations and doesn't do field type validations etc
     *
     */
    private CompositeField buildCompositeField(String field, Settings compositeFieldSettings) {
        List<Dimension> dimensions = new ArrayList<>();
        List<Metric> metrics = new ArrayList<>();
        List<String> dimensionsOrder = compositeFieldSettings.getAsList(DIMENSIONS_ORDER);
        if (dimensionsOrder == null || dimensionsOrder.isEmpty()) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "dimensions_order is required for composite index field [%s]", field)
            );
        }
        if (dimensionsOrder.size() < 2) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Atleast two dimensions are required to build composite index field [%s]", field)
            );
        }

        Map<String, Settings> dimConfig = compositeFieldSettings.getGroups(DIMENSIONS_CONFIG);

        for (String dimension : dimConfig.keySet()) {
            if (!dimensionsOrder.contains(dimension)) {
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "dimension [%s] is not present in dimensions_order for composite index field [%s]",
                        dimension,
                        field
                    )
                );
            }
        }

        List<String> metricFields = compositeFieldSettings.getAsList(METRICS);
        if (metricFields == null || metricFields.isEmpty()) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "metrics is required for composite index field [%s]", field));
        }
        Map<String, Settings> metricsConfig = compositeFieldSettings.getGroups(METRICS_CONFIG);

        for (String metricField : metricsConfig.keySet()) {
            if (!metricFields.contains(metricField)) {
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "metric field [%s] is not present in 'metrics' for composite index field [%s]",
                        metricField,
                        field
                    )
                );
            }
        }

        Set<String> uniqueDimensions = new HashSet<>();
        for (String dimension : dimensionsOrder) {
            if (!uniqueDimensions.add(dimension)) {
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "duplicate dimension [%s] found in dimensions_order for composite index field [%s]",
                        dimension,
                        field
                    )
                );
            }
            dimensions.add(DimensionFactory.create(dimension, dimConfig.get(dimension)));
        }
        uniqueDimensions = null;
        Set<String> uniqueMetricFields = new HashSet<>();
        for (String metricField : metricFields) {
            if (!uniqueMetricFields.add(metricField)) {
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "duplicate metric field [%s] found in 'metrics' for composite index field [%s]",
                        metricField,
                        field
                    )
                );
            }
            Settings metricSettings = metricsConfig.get(metricField);
            if (metricSettings == null) {
                // fill cluster level defaults in create flow as part of CompositeIndexSupplier
                metrics.add(new Metric(metricField, new ArrayList<>()));
            } else {
                String name = metricSettings.get(FIELD, metricField);
                List<String> metricsList = metricSettings.getAsList(METRICS);
                if (metricsList.isEmpty()) {
                    // fill cluster level defaults in create flow as part of CompositeIndexSupplier
                    metrics.add(new Metric(name, new ArrayList<>()));
                } else {
                    List<MetricType> metricTypes = new ArrayList<>();
                    Set<String> uniqueMetricTypes = new HashSet<>();
                    for (String metric : metricsList) {
                        if (!uniqueMetricTypes.add(metric)) {
                            throw new IllegalArgumentException(
                                String.format(
                                    Locale.ROOT,
                                    "duplicate metric type [%s] found in metrics for composite index field [%s]",
                                    metric,
                                    field
                                )
                            );
                        }
                        metricTypes.add(MetricType.fromTypeName(metric));
                    }
                    uniqueMetricTypes = null;
                    metrics.add(new Metric(name, metricTypes));
                }
            }
        }
        uniqueMetricFields = null;

        IndexMode indexMode = IndexMode.fromTypeName(compositeFieldSettings.get(INDEX_MODE, IndexMode.STARTREE.typeName));
        Settings fieldSpec = compositeFieldSettings.getAsSettings(indexMode.typeName + SPEC);
        CompositeFieldSpec compositeFieldSpec = CompositeFieldSpecFactory.create(indexMode, fieldSpec, dimensionsOrder);
        return new CompositeField(field, dimensions, metrics, compositeFieldSpec);
    }

    public static Rounding.DateTimeUnit getTimeUnit(String expression) {
        if (!DateHistogramAggregationBuilder.DATE_FIELD_UNITS.containsKey(expression)) {
            throw new IllegalArgumentException("unknown calendar interval specified in composite index config");
        }
        return DateHistogramAggregationBuilder.DATE_FIELD_UNITS.get(expression);
    }

    /**
     * Dimension factory based on field type
     */
    private static class DimensionFactory {
        static Dimension create(String dimension, Settings settings) {
            if (settings == null) {
                return new Dimension(dimension);
            }
            String field = settings.get(FIELD, dimension);
            String type = settings.get(TYPE, DimensionType.DEFAULT.getTypeName());
            switch (DimensionType.fromTypeName(type)) {
                case DEFAULT:
                    return new Dimension(field);
                case DATE:
                    return new DateDimension(field, settings);
                default:
                    throw new IllegalArgumentException(
                        String.format(Locale.ROOT, "Invalid dimension type [%s] in composite index config", type)
                    );
            }
        }

        static Dimension createEmptyMappedDimension(Dimension dimension, MappedFieldType type) {
            if (type instanceof DateFieldMapper.DateFieldType) {
                return new DateDimension(dimension.getField(), new ArrayList<>());
            }
            return dimension;
        }
    }

    /**
     * The type of dimension source fields
     * Default fields are of Numeric type
     */
    private enum DimensionType {
        DEFAULT("default"),
        DATE("date");

        private final String typeName;

        DimensionType(String typeName) {
            this.typeName = typeName;
        }

        public String getTypeName() {
            return typeName;
        }

        public static DimensionType fromTypeName(String typeName) {
            for (DimensionType dimensionType : DimensionType.values()) {
                if (dimensionType.getTypeName().equalsIgnoreCase(typeName)) {
                    return dimensionType;
                }
            }
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Invalid dimension type in composite index config: [%s] ", typeName)
            );
        }
    }

    /**
     * Composite field spec factory based on index mode
     */
    private static class CompositeFieldSpecFactory {
        static CompositeFieldSpec create(IndexMode indexMode, Settings settings, List<String> dimensions) {
            if (settings == null) {
                return new StarTreeFieldSpec(10000, new ArrayList<>(), StarTreeFieldSpec.StarTreeBuildMode.OFF_HEAP);
            }
            switch (indexMode) {
                case STARTREE:
                    return buildStarTreeFieldSpec(settings, dimensions);
                default:
                    throw new IllegalArgumentException(
                        String.format(Locale.ROOT, "Invalid index mode [%s] in composite index config", indexMode)
                    );
            }
        }
    }

    private static StarTreeFieldSpec buildStarTreeFieldSpec(Settings settings, List<String> dimensions) {
        StarTreeFieldSpec.StarTreeBuildMode buildMode = StarTreeFieldSpec.StarTreeBuildMode.fromTypeName(
            settings.get(STAR_TREE_BUILD_MODE, StarTreeFieldSpec.StarTreeBuildMode.OFF_HEAP.getTypeName())
        );
        // Fill default value as part of create flow as part of supplier
        int maxLeafDocs = settings.getAsInt(MAX_LEAF_DOCS, Integer.MAX_VALUE);
        if (maxLeafDocs < 1) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Invalid max_leaf_docs [%s] in composite index config", maxLeafDocs)
            );
        }
        List<String> skipStarNodeCreationInDims = settings.getAsList(SKIP_STAR_NODE_CREATION_FOR_DIMS, new ArrayList<>());
        for (String dim : skipStarNodeCreationInDims) {
            if (!dimensions.contains(dim)) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "Invalid dimension [%s] in skip_star_node_creation_for_dims", dim)
                );
            }
        }
        return new StarTreeFieldSpec(maxLeafDocs, skipStarNodeCreationInDims, buildMode);
    }

    /**
     * Enum for index mode of the underlying composite index
     * The default and only index supported right now is star tree index
     */
    private enum IndexMode {
        STARTREE("startree");

        private final String typeName;

        IndexMode(String typeName) {
            this.typeName = typeName;
        }

        public String getTypeName() {
            return typeName;
        }

        public static IndexMode fromTypeName(String typeName) {
            for (IndexMode indexType : IndexMode.values()) {
                if (indexType.getTypeName().equalsIgnoreCase(typeName)) {
                    return indexType;
                }
            }
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Invalid index mode in composite index config: [%s] ", typeName));
        }
    }

    /**
     * Returns the composite fields built based on compositeIndexConfig index settings
     */
    public List<CompositeField> getCompositeFields() {
        return compositeFields;
    }

    /**
     * Returns whether there are any composite fields as part of the compositeIndexConfig
     */
    public boolean hasCompositeFields() {
        return !compositeFields.isEmpty();
    }

    /**
     * Validates the composite fields based on IndexSettingDefaults and the mappedFieldType
     * Updates CompositeIndexConfig with newer, completely updated composite fields
     *
     */
    public CompositeIndexConfig validateAndGetCompositeIndexConfig(
        Function<String, MappedFieldType> fieldTypeLookup,
        CompositeIndexSettings compositeIndexSettings
    ) {
        if (hasCompositeFields() == false) {
            return null;
        }
        if (!compositeIndexSettings.isEnabled()) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "composite index cannot be created, enable it using [%s] setting",
                    COMPOSITE_INDEX_ENABLED_SETTING.getKey()
                )
            );
        }
        if (compositeFields.size() > compositeIndexSettings.getMaxFields()) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "composite index can have atmost [%s] fields", compositeIndexSettings.getMaxFields())
            );
        }
        List<CompositeField> validatedAndMappedCompositeFields = new ArrayList<>();
        for (CompositeField compositeField : compositeFields) {
            if (compositeField.getDimensionsOrder().size() > compositeIndexSettings.getMaxDimensions()) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "composite index can have atmost [%s] dimensions", compositeIndexSettings.getMaxDimensions())
                );
            }
            List<Dimension> dimensions = new ArrayList<>();
            for (Dimension dimension : compositeField.getDimensionsOrder()) {
                validateCompositeDimensionField(dimension.getField(), fieldTypeLookup, compositeField.getName());
                dimension = mapDimension(dimension, fieldTypeLookup.apply(dimension.getField()));
                dimension.setDefaults(compositeIndexSettings);
                dimensions.add(dimension);
            }
            List<Metric> metrics = new ArrayList<>();
            for (Metric metric : compositeField.getMetrics()) {
                validateCompositeMetricField(metric.getField(), fieldTypeLookup, compositeField.getName());
                metric.setDefaults(compositeIndexSettings);
                metrics.add(metric);
            }
            compositeField.getSpec().setDefaults(compositeIndexSettings);
            validatedAndMappedCompositeFields.add(
                new CompositeField(compositeField.getName(), dimensions, metrics, compositeField.getSpec())
            );
        }
        this.compositeFields.clear();
        this.compositeFields.addAll(validatedAndMappedCompositeFields);
        return this;
    }

    /**
     * Maps the dimension to right dimension type based on MappedFieldType
     */
    private Dimension mapDimension(Dimension dimension, MappedFieldType fieldType) {
        if (!isDimensionMappedToFieldType(dimension, fieldType)) {
            return DimensionFactory.createEmptyMappedDimension(dimension, fieldType);
        }
        return dimension;
    }

    /**
     * Checks whether dimension field type is same as the source field type
     */
    private boolean isDimensionMappedToFieldType(Dimension dimension, MappedFieldType fieldType) {
        if (fieldType instanceof DateFieldMapper.DateFieldType) {
            return dimension instanceof DateDimension;
        }
        return true;
    }

    /**
     * Validations :
     * The dimension field should be one of the source fields of the index
     * The dimension fields must be aggregation compatible (doc values + field data supported)
     * The dimension fields should be of numberField type / dateField type
     *
     */
    private void validateCompositeDimensionField(
        String field,
        Function<String, MappedFieldType> fieldTypeLookup,
        String compositeFieldName
    ) {
        final MappedFieldType ft = fieldTypeLookup.apply(field);
        if (ft == null) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "unknown dimension field [%s] as part of composite field [%s]", field, compositeFieldName)
            );
        }
        if (!isAllowedDimensionFieldType(ft)) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "composite index is not supported for the dimension field [%s] with field type [%s] as part of "
                        + "composite field [%s]",
                    field,
                    ft.typeName(),
                    compositeFieldName
                )
            );
        }
        // doc values not present / field data not supported
        if (!ft.isAggregatable()) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "Aggregations not supported for the dimension field [%s] with field type [%s] as part of " + "composite field [%s]",
                    field,
                    ft.typeName(),
                    compositeFieldName
                )
            );
        }
    }

    /**
     * Validations :
     * The metric field should be one of the source fields of the index
     * The metric fields must be aggregation compatible (doc values + field data supported)
     * The metric fields should be of numberField type
     *
     */
    private void validateCompositeMetricField(String field, Function<String, MappedFieldType> fieldTypeLookup, String compositeFieldName) {
        final MappedFieldType ft = fieldTypeLookup.apply(field);
        if (ft == null) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "unknown metric field [%s] as part of composite field [%s]", field, compositeFieldName)
            );
        }
        if (!isAllowedMetricFieldType(ft)) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "composite index is not supported for the metric field [%s] with field type [%s] as part of " + "composite field [%s]",
                    field,
                    ft.typeName(),
                    compositeFieldName
                )
            );
        }
        if (!ft.isAggregatable()) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "Aggregations not supported for the composite index metric field [%s] with field type [%s] as part of "
                        + "composite field [%s]",
                    field,
                    ft.typeName(),
                    compositeFieldName
                )
            );
        }
    }

    private static boolean isAllowedDimensionFieldType(MappedFieldType fieldType) {
        return ALLOWED_DIMENSION_MAPPED_FIELD_TYPES.stream().anyMatch(allowedType -> allowedType.isInstance(fieldType));
    }

    private static boolean isAllowedMetricFieldType(MappedFieldType fieldType) {
        return ALLOWED_METRIC_MAPPED_FIELD_TYPES.stream().anyMatch(allowedType -> allowedType.isInstance(fieldType));
    }
}
