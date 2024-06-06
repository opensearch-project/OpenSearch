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
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.indices.IndicesService;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

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

    /**
     * This setting determines the max number of composite fields that can be part of composite index config. For each
     * composite field, we will generate associated composite index. (eg : star tree index per field )
     */
    public static final Setting<Integer> COMPOSITE_INDEX_MAX_FIELDS_SETTING = Setting.intSetting(
        "index.composite_index.max_fields",
        1,
        1,
        1,
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    /**
     * This setting determines the max number of dimensions that can be part of composite index field. Number of
     * dimensions and associated cardinality has direct effect of composite index size and query performance.
     */
    public static final Setting<Integer> COMPOSITE_INDEX_MAX_DIMENSIONS_SETTING = Setting.intSetting(
        "index.composite_index.field.max_dimensions",
        10,
        2,
        10,
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    /**
     * This setting configures the default "maxLeafDocs" setting of star tree. This affects both query performance and
     * star tree index size. Lesser the leaves, better the query latency but higher storage size and vice versa
     * <p>
     * We can remove this later or change it to an enum based constant setting.
     *
     * @opensearch.experimental
     */
    public static final Setting<Integer> STAR_TREE_DEFAULT_MAX_LEAF_DOCS = Setting.intSetting(
        "index.composite_index.startree.default.max_leaf_docs",
        10000,
        1,
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    /**
     * Default intervals for date dimension as part of composite fields
     */
    public static final Setting<List<Rounding.DateTimeUnit>> DEFAULT_DATE_INTERVALS = Setting.listSetting(
        "index.composite_index.field.default.date_intervals",
        Arrays.asList(Rounding.DateTimeUnit.MINUTES_OF_HOUR.shortName(), Rounding.DateTimeUnit.HOUR_OF_DAY.shortName()),
        CompositeIndexConfig::getTimeUnit,
        Setting.Property.IndexScope,
        Setting.Property.Final
    );
    public static final Setting<List<MetricType>> DEFAULT_METRICS_LIST = Setting.listSetting(
        "index.composite_index.field.default.metrics",
        Arrays.asList(
            MetricType.AVG.toString(),
            MetricType.COUNT.toString(),
            MetricType.SUM.toString(),
            MetricType.MAX.toString(),
            MetricType.MIN.toString()
        ),
        MetricType::fromTypeName,
        Setting.Property.IndexScope,
        Setting.Property.Final
    );
    private volatile int maxLeafDocs;
    private volatile List<Rounding.DateTimeUnit> defaultDateIntervals;
    private volatile List<MetricType> defaultMetrics;
    private volatile int maxDimensions;
    private volatile int maxFields;
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
    private final IndexSettings indexSettings;

    public CompositeIndexConfig(IndexSettings indexSettings) {
        this.setMaxLeafDocs(indexSettings.getValue(STAR_TREE_DEFAULT_MAX_LEAF_DOCS));
        this.setDefaultDateIntervals(indexSettings.getValue(DEFAULT_DATE_INTERVALS));
        this.setDefaultMetrics(indexSettings.getValue(DEFAULT_METRICS_LIST));
        this.setMaxDimensions(indexSettings.getValue(COMPOSITE_INDEX_MAX_DIMENSIONS_SETTING));
        this.setMaxFields(indexSettings.getValue(COMPOSITE_INDEX_MAX_FIELDS_SETTING));
        final Map<String, Settings> compositeIndexSettings = indexSettings.getSettings().getGroups(COMPOSITE_INDEX_CONFIG);
        this.indexSettings = indexSettings;
        Set<String> fields = compositeIndexSettings.keySet();
        if (!fields.isEmpty()) {
            if (!FeatureFlags.isEnabled(FeatureFlags.COMPOSITE_INDEX_SETTING)) {
                throw new IllegalArgumentException(
                    "star tree index is under an experimental feature and can be activated only by enabling "
                        + FeatureFlags.COMPOSITE_INDEX_SETTING.getKey()
                        + " feature flag in the JVM options"
                );
            }
            if (fields.size() > getMaxFields()) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "composite index can have atmost [%s] fields", getMaxFields())
                );
            }
        }
        for (String field : fields) {
            compositeFields.add(buildCompositeField(field, compositeIndexSettings.get(field)));
        }
    }

    /**
     * This returns composite field after performing basic validations but doesn't perform field type based validations
     *
     */
    private CompositeField buildCompositeField(String field, Settings compositeFieldSettings) {

        List<Dimension> dimensions = buildDimensions(field, compositeFieldSettings);
        List<Metric> metrics = buildMetrics(field, compositeFieldSettings);
        List<String> dimensionsOrder = compositeFieldSettings.getAsList(DIMENSIONS_ORDER);
        IndexMode indexMode = IndexMode.fromTypeName(compositeFieldSettings.get(INDEX_MODE, IndexMode.STARTREE.typeName));
        Settings fieldSpec = compositeFieldSettings.getAsSettings(indexMode.typeName + SPEC);
        CompositeFieldSpec compositeFieldSpec = CompositeFieldSpecFactory.create(indexMode, fieldSpec, dimensionsOrder, dimensions, this);
        return new CompositeField(field, dimensions, metrics, compositeFieldSpec);
    }

    /**
     * Returns dimensions after performing validations
     *
     */
    private List<Dimension> buildDimensions(String field, Settings compositeFieldSettings) {
        List<Dimension> dimensions = new ArrayList<>();
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

        if (dimensionsOrder.size() > getMaxDimensions()) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "composite index can have atmost [%s] dimensions", getMaxDimensions())
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
            dimensions.add(DimensionFactory.create(dimension, dimConfig.get(dimension), this));
        }
        return dimensions;
    }

    /**
     * Dimension factory based on field type
     */
    private static class DimensionFactory {
        static Dimension create(String dimension, Settings settings, CompositeIndexConfig compositeIndexConfig) {
            if (settings == null) {
                return new Dimension(dimension);
            }
            String field = settings.get(FIELD, dimension);
            String type = settings.get(TYPE, DimensionType.DEFAULT.getTypeName());
            switch (DimensionType.fromTypeName(type)) {
                case DEFAULT:
                    return new Dimension(field);
                case DATE:
                    return new DateDimension(field, settings, compositeIndexConfig);
                default:
                    throw new IllegalArgumentException(
                        String.format(Locale.ROOT, "Invalid dimension type [%s] in composite index config", type)
                    );
            }
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
     * Returns metrics after performing validations
     *
     */
    private List<Metric> buildMetrics(String field, Settings compositeFieldSettings) {
        List<Metric> metrics = new ArrayList<>();
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
                metrics.add(new Metric(metricField, getDefaultMetrics()));
            } else {
                String name = metricSettings.get(FIELD, metricField);
                List<String> metricsList = metricSettings.getAsList(METRICS);
                if (metricsList.isEmpty()) {
                    metrics.add(new Metric(name, getDefaultMetrics()));
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
                    metrics.add(new Metric(name, metricTypes));
                }
            }
        }
        return metrics;
    }

    /**
     * Composite field spec factory based on index mode
     */
    private static class CompositeFieldSpecFactory {
        static CompositeFieldSpec create(
            IndexMode indexMode,
            Settings settings,
            List<String> dimensionsOrder,
            List<Dimension> dimensions,
            CompositeIndexConfig compositeIndexConfig
        ) {
            if (settings == null) {
                return new StarTreeFieldSpec(
                    compositeIndexConfig.getMaxLeafDocs(),
                    new ArrayList<>(),
                    StarTreeFieldSpec.StarTreeBuildMode.OFF_HEAP
                );
            }
            switch (indexMode) {
                case STARTREE:
                    return buildStarTreeFieldSpec(settings, dimensionsOrder, dimensions, compositeIndexConfig);
                default:
                    throw new IllegalArgumentException(
                        String.format(Locale.ROOT, "Invalid index mode [%s] in composite index config", indexMode)
                    );
            }
        }
    }

    private static StarTreeFieldSpec buildStarTreeFieldSpec(
        Settings settings,
        List<String> dimensionsString,
        List<Dimension> dimensions,
        CompositeIndexConfig compositeIndexConfig
    ) {
        StarTreeFieldSpec.StarTreeBuildMode buildMode = StarTreeFieldSpec.StarTreeBuildMode.fromTypeName(
            settings.get(STAR_TREE_BUILD_MODE, StarTreeFieldSpec.StarTreeBuildMode.OFF_HEAP.getTypeName())
        );
        int maxLeafDocs = settings.getAsInt(MAX_LEAF_DOCS, compositeIndexConfig.getMaxLeafDocs());
        if (maxLeafDocs < 1) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Invalid max_leaf_docs [%s] in composite index config", maxLeafDocs)
            );
        }
        List<String> skipStarNodeCreationInDims = settings.getAsList(SKIP_STAR_NODE_CREATION_FOR_DIMS, new ArrayList<>());
        Set<String> skipListWithMappedFieldNames = new HashSet<>();
        for (String dim : skipStarNodeCreationInDims) {
            if (!dimensionsString.contains(dim)) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "Invalid dimension [%s] in skip_star_node_creation_for_dims", dim)
                );
            }
            boolean duplicate = !(skipListWithMappedFieldNames.add(dimensions.get(dimensionsString.indexOf(dim)).getField()));
            if (duplicate) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "duplicate dimension [%s] found in skipStarNodeCreationInDims", dim)
                );
            }
        }
        return new StarTreeFieldSpec(maxLeafDocs, new ArrayList<>(skipListWithMappedFieldNames), buildMode);
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
     * Validates the composite fields based on defaults and based on the mappedFieldType
     * Updates CompositeIndexConfig with newer, completely updated composite fields.
     *
     */
    public CompositeIndexConfig validateAndGetCompositeIndexConfig(
        Function<String, MappedFieldType> fieldTypeLookup,
        BooleanSupplier isCompositeIndexCreationEnabled
    ) {
        if (hasCompositeFields() == false) {
            return null;
        }
        if (!isCompositeIndexCreationEnabled.getAsBoolean()) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "composite index cannot be created, enable it using [%s] setting",
                    IndicesService.COMPOSITE_INDEX_ENABLED_SETTING.getKey()
                )
            );
        }
        for (CompositeField compositeField : compositeFields) {
            for (Dimension dimension : compositeField.getDimensionsOrder()) {
                validateDimensionField(dimension, fieldTypeLookup, compositeField.getName());
            }
            for (Metric metric : compositeField.getMetrics()) {
                validateMetricField(metric.getField(), fieldTypeLookup, compositeField.getName());
            }
        }
        return this;
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
    private void validateDimensionField(Dimension dimension, Function<String, MappedFieldType> fieldTypeLookup, String compositeFieldName) {
        final MappedFieldType ft = fieldTypeLookup.apply(dimension.getField());
        if (ft == null) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "unknown dimension field [%s] as part of composite field [%s]",
                    dimension.getField(),
                    compositeFieldName
                )
            );
        }
        if (!isDimensionMappedToFieldType(dimension, ft)) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "specify field type [%s] for dimension field [%s] as part of of composite field [%s]",
                    ft.typeName(),
                    dimension.getField(),
                    compositeFieldName
                )
            );
        }
        if (!isAllowedDimensionFieldType(ft)) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "composite index is not supported for the dimension field [%s] with field type [%s] as part of "
                        + "composite field [%s]",
                    dimension.getField(),
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
                    dimension.getField(),
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
    private void validateMetricField(String field, Function<String, MappedFieldType> fieldTypeLookup, String compositeFieldName) {
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

    public static Rounding.DateTimeUnit getTimeUnit(String expression) {
        if (!DateHistogramAggregationBuilder.DATE_FIELD_UNITS.containsKey(expression)) {
            throw new IllegalArgumentException("unknown calendar interval specified in composite index config");
        }
        return DateHistogramAggregationBuilder.DATE_FIELD_UNITS.get(expression);
    }

    public void setMaxLeafDocs(int maxLeafDocs) {
        this.maxLeafDocs = maxLeafDocs;
    }

    public void setDefaultDateIntervals(List<Rounding.DateTimeUnit> defaultDateIntervals) {
        this.defaultDateIntervals = defaultDateIntervals;
    }

    public void setDefaultMetrics(List<MetricType> defaultMetrics) {
        this.defaultMetrics = defaultMetrics;
    }

    public void setMaxDimensions(int maxDimensions) {
        this.maxDimensions = maxDimensions;
    }

    public void setMaxFields(int maxFields) {
        this.maxFields = maxFields;
    }

    public int getMaxDimensions() {
        return maxDimensions;
    }

    public int getMaxFields() {
        return maxFields;
    }

    public int getMaxLeafDocs() {
        return maxLeafDocs;
    }

    public List<Rounding.DateTimeUnit> getDefaultDateIntervals() {
        return defaultDateIntervals;
    }

    public List<MetricType> getDefaultMetrics() {
        return defaultMetrics;
    }

}
