/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.search.Query;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.xcontent.support.XContentMapValues;
import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.index.compositeindex.datacube.DimensionFactory;
import org.opensearch.index.compositeindex.datacube.Metric;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeFieldConfiguration;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeIndexSettings;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.lookup.SearchLookup;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A field mapper for star tree fields
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class StarTreeMapper extends ParametrizedFieldMapper {
    public static final String CONTENT_TYPE = "star_tree";
    public static final String CONFIG = "config";
    public static final String MAX_LEAF_DOCS = "max_leaf_docs";
    public static final String SKIP_STAR_NODE_IN_DIMS = "skip_star_node_creation_for_dimensions";
    public static final String BUILD_MODE = "build_mode";
    public static final String ORDERED_DIMENSIONS = "ordered_dimensions";
    public static final String METRICS = "metrics";
    public static final String STATS = "stats";

    @Override
    public ParametrizedFieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName(), objBuilder).init(this);

    }

    /**
     * Builder for the star tree field mapper
     *
     * @opensearch.internal
     */
    public static class Builder extends ParametrizedFieldMapper.Builder {
        private ObjectMapper.Builder objbuilder;
        private static final Set<Class<? extends Mapper.Builder>> ALLOWED_DIMENSION_MAPPER_BUILDERS = Set.of(
            NumberFieldMapper.Builder.class,
            DateFieldMapper.Builder.class
        );
        private static final Set<Class<? extends Mapper.Builder>> ALLOWED_METRIC_MAPPER_BUILDERS = Set.of(NumberFieldMapper.Builder.class);

        @SuppressWarnings("unchecked")
        private final Parameter<StarTreeField> config = new Parameter<>(CONFIG, false, () -> null, (name, context, nodeObj) -> {
            if (nodeObj instanceof Map<?, ?>) {
                Map<String, Object> paramMap = (Map<String, Object>) nodeObj;
                int maxLeafDocs = XContentMapValues.nodeIntegerValue(
                    paramMap.get(MAX_LEAF_DOCS),
                    StarTreeIndexSettings.STAR_TREE_DEFAULT_MAX_LEAF_DOCS.get(context.getSettings())
                );
                if (maxLeafDocs < 1) {
                    throw new IllegalArgumentException(
                        String.format(Locale.ROOT, "%s [%s] must be greater than 0", MAX_LEAF_DOCS, maxLeafDocs)
                    );
                }
                paramMap.remove(MAX_LEAF_DOCS);
                Set<String> skipStarInDims = new LinkedHashSet<>(
                    List.of(XContentMapValues.nodeStringArrayValue(paramMap.getOrDefault(SKIP_STAR_NODE_IN_DIMS, new ArrayList<String>())))
                );
                paramMap.remove(SKIP_STAR_NODE_IN_DIMS);
                // TODO : change this to off heap once off heap gets implemented
                StarTreeFieldConfiguration.StarTreeBuildMode buildMode = StarTreeFieldConfiguration.StarTreeBuildMode.ON_HEAP;

                List<Dimension> dimensions = buildDimensions(name, paramMap, context);
                paramMap.remove(ORDERED_DIMENSIONS);
                List<Metric> metrics = buildMetrics(name, paramMap, context);
                paramMap.remove(METRICS);
                paramMap.remove(CompositeDataCubeFieldType.NAME);
                for (String dim : skipStarInDims) {
                    if (dimensions.stream().filter(d -> d.getField().equals(dim)).findAny().isEmpty()) {
                        throw new IllegalArgumentException(
                            String.format(
                                Locale.ROOT,
                                "[%s] in skip_star_node_creation_for_dimensions should be part of ordered_dimensions",
                                dim
                            )
                        );
                    }
                }
                StarTreeFieldConfiguration spec = new StarTreeFieldConfiguration(maxLeafDocs, skipStarInDims, buildMode);
                DocumentMapperParser.checkNoRemainingFields(
                    paramMap,
                    context.indexVersionCreated(),
                    "Star tree mapping definition has unsupported parameters: "
                );
                return new StarTreeField(this.name, dimensions, metrics, spec);

            } else {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "unable to parse config for star tree field [%s]", this.name)
                );
            }
        }, m -> toType(m).starTreeField);

        /**
         * Build dimensions from mapping
         */
        @SuppressWarnings("unchecked")
        private List<Dimension> buildDimensions(String fieldName, Map<String, Object> map, Mapper.TypeParser.ParserContext context) {
            Object dims = XContentMapValues.extractValue("ordered_dimensions", map);
            if (dims == null) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "ordered_dimensions is required for star tree field [%s]", fieldName)
                );
            }
            List<Dimension> dimensions = new LinkedList<>();
            if (dims instanceof List<?>) {
                List<Object> dimList = (List<Object>) dims;
                if (dimList.size() > context.getSettings()
                    .getAsInt(
                        StarTreeIndexSettings.STAR_TREE_MAX_DIMENSIONS_SETTING.getKey(),
                        StarTreeIndexSettings.STAR_TREE_MAX_DIMENSIONS_DEFAULT
                    )) {
                    throw new IllegalArgumentException(
                        String.format(
                            Locale.ROOT,
                            "ordered_dimensions cannot have more than %s dimensions for star tree field [%s]",
                            context.getSettings()
                                .getAsInt(
                                    StarTreeIndexSettings.STAR_TREE_MAX_DIMENSIONS_SETTING.getKey(),
                                    StarTreeIndexSettings.STAR_TREE_MAX_DIMENSIONS_DEFAULT
                                ),
                            fieldName
                        )
                    );
                }
                if (dimList.size() < 2) {
                    throw new IllegalArgumentException(
                        String.format(Locale.ROOT, "Atleast two dimensions are required to build star tree index field [%s]", fieldName)
                    );
                }
                for (Object dim : dimList) {
                    dimensions.add(getDimension(fieldName, dim, context));
                }
            } else {
                throw new MapperParsingException(
                    String.format(Locale.ROOT, "unable to parse ordered_dimensions for star tree field [%s]", fieldName)
                );
            }
            return dimensions;
        }

        /**
         * Get dimension based on mapping
         */
        @SuppressWarnings("unchecked")
        private Dimension getDimension(String fieldName, Object dimensionMapping, Mapper.TypeParser.ParserContext context) {
            Dimension dimension;
            Map<String, Object> dimensionMap = (Map<String, Object>) dimensionMapping;
            String name = (String) XContentMapValues.extractValue(CompositeDataCubeFieldType.NAME, dimensionMap);
            dimensionMap.remove(CompositeDataCubeFieldType.NAME);
            if (this.objbuilder == null || this.objbuilder.mappersBuilders == null) {
                String type = (String) XContentMapValues.extractValue(CompositeDataCubeFieldType.TYPE, dimensionMap);
                dimensionMap.remove(CompositeDataCubeFieldType.TYPE);
                if (type == null) {
                    throw new MapperParsingException(
                        String.format(Locale.ROOT, "unable to parse ordered_dimensions for star tree field [%s]", fieldName)
                    );
                }
                return DimensionFactory.parseAndCreateDimension(name, type, dimensionMap, context);
            } else {
                Optional<Mapper.Builder> dimBuilder = findMapperBuilderByName(name, this.objbuilder.mappersBuilders);
                if (dimBuilder.isEmpty()) {
                    throw new IllegalArgumentException(String.format(Locale.ROOT, "unknown dimension field [%s]", name));
                }
                if (!isBuilderAllowedForDimension(dimBuilder.get())) {
                    throw new IllegalArgumentException(
                        String.format(
                            Locale.ROOT,
                            "unsupported field type associated with dimension [%s] as part of star tree field [%s]",
                            name,
                            fieldName
                        )
                    );
                }
                dimension = DimensionFactory.parseAndCreateDimension(name, dimBuilder.get(), dimensionMap, context);
            }
            DocumentMapperParser.checkNoRemainingFields(
                dimensionMap,
                context.indexVersionCreated(),
                "Star tree mapping definition has unsupported parameters: "
            );
            return dimension;
        }

        /**
         * Build metrics from mapping
         */
        @SuppressWarnings("unchecked")
        private List<Metric> buildMetrics(String fieldName, Map<String, Object> map, Mapper.TypeParser.ParserContext context) {
            List<Metric> metrics = new LinkedList<>();
            Object metricsFromInput = XContentMapValues.extractValue(METRICS, map);
            if (metricsFromInput == null) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "metrics section is required for star tree field [%s]", fieldName)
                );
            }
            if (metricsFromInput instanceof List<?>) {
                List<?> metricsList = (List<?>) metricsFromInput;
                for (Object metric : metricsList) {
                    Map<String, Object> metricMap = (Map<String, Object>) metric;
                    String name = (String) XContentMapValues.extractValue(CompositeDataCubeFieldType.NAME, metricMap);
                    metricMap.remove(CompositeDataCubeFieldType.NAME);
                    if (objbuilder == null || objbuilder.mappersBuilders == null) {
                        metrics.add(getMetric(name, metricMap, context));
                    } else {
                        Optional<Mapper.Builder> meticBuilder = findMapperBuilderByName(name, this.objbuilder.mappersBuilders);
                        if (meticBuilder.isEmpty()) {
                            throw new IllegalArgumentException(String.format(Locale.ROOT, "unknown metric field [%s]", name));
                        }
                        if (!isBuilderAllowedForMetric(meticBuilder.get())) {
                            throw new IllegalArgumentException(
                                String.format(Locale.ROOT, "non-numeric field type is associated with star tree metric [%s]", this.name)
                            );
                        }
                        metrics.add(getMetric(name, metricMap, context));
                        DocumentMapperParser.checkNoRemainingFields(
                            metricMap,
                            context.indexVersionCreated(),
                            "Star tree mapping definition has unsupported parameters: "
                        );
                    }
                }
            } else {
                throw new MapperParsingException(String.format(Locale.ROOT, "unable to parse metrics for star tree field [%s]", this.name));
            }

            return metrics;
        }

        @SuppressWarnings("unchecked")
        private Metric getMetric(String name, Map<String, Object> metric, Mapper.TypeParser.ParserContext context) {
            List<MetricStat> metricTypes;
            List<String> metricStrings = XContentMapValues.extractRawValues(STATS, metric)
                .stream()
                .map(Object::toString)
                .collect(Collectors.toList());
            metric.remove(STATS);
            if (metricStrings.isEmpty()) {
                metricTypes = new ArrayList<>(StarTreeIndexSettings.DEFAULT_METRICS_LIST.get(context.getSettings()));
            } else {
                Set<MetricStat> metricSet = new LinkedHashSet<>();
                for (String metricString : metricStrings) {
                    metricSet.add(MetricStat.fromTypeName(metricString));
                }
                metricTypes = new ArrayList<>(metricSet);
            }
            return new Metric(name, metricTypes);
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return List.of(config);
        }

        private static boolean isBuilderAllowedForDimension(Mapper.Builder builder) {
            return ALLOWED_DIMENSION_MAPPER_BUILDERS.stream().anyMatch(allowedType -> allowedType.isInstance(builder));
        }

        private static boolean isBuilderAllowedForMetric(Mapper.Builder builder) {
            return ALLOWED_METRIC_MAPPER_BUILDERS.stream().anyMatch(allowedType -> allowedType.isInstance(builder));
        }

        private Optional<Mapper.Builder> findMapperBuilderByName(String field, List<Mapper.Builder> mappersBuilders) {
            return mappersBuilders.stream().filter(builder -> builder.name().equals(field)).findFirst();
        }

        public Builder(String name, ObjectMapper.Builder objBuilder) {
            super(name);
            this.objbuilder = objBuilder;
        }

        @Override
        public ParametrizedFieldMapper build(BuilderContext context) {
            StarTreeFieldType type = new StarTreeFieldType(name, this.config.get());
            return new StarTreeMapper(name, type, this, objbuilder);
        }
    }

    private static StarTreeMapper toType(FieldMapper in) {
        return (StarTreeMapper) in;
    }

    /**
     * Concrete parse for star tree type
     *
     * @opensearch.internal
     */
    public static class TypeParser implements Mapper.TypeParser {

        /**
         * default constructor of VectorFieldMapper.TypeParser
         */
        public TypeParser() {}

        @Override
        public Mapper.Builder<?> parse(String name, Map<String, Object> node, ParserContext context) throws MapperParsingException {
            Builder builder = new StarTreeMapper.Builder(name, null);
            builder.parse(name, context, node);
            return builder;
        }

        @Override
        public Mapper.Builder<?> parse(String name, Map<String, Object> node, ParserContext context, ObjectMapper.Builder objBuilder)
            throws MapperParsingException {
            Builder builder = new StarTreeMapper.Builder(name, objBuilder);
            builder.parse(name, context, node);
            return builder;
        }
    }

    private final StarTreeField starTreeField;

    private final ObjectMapper.Builder objBuilder;

    protected StarTreeMapper(String simpleName, StarTreeFieldType type, Builder builder, ObjectMapper.Builder objbuilder) {
        super(simpleName, type, MultiFields.empty(), CopyTo.empty());
        this.starTreeField = builder.config.get();
        this.objBuilder = objbuilder;
    }

    @Override
    public StarTreeFieldType fieldType() {
        return (StarTreeFieldType) super.fieldType();
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected void parseCreateField(ParseContext context) {
        throw new MapperParsingException(
            String.format(
                Locale.ROOT,
                "Field [%s] is a star tree field and cannot be added inside a document. Use the index API request parameters.",
                name()
            )
        );
    }

    /**
     * Star tree mapped field type containing dimensions, metrics, star tree specs
     *
     * @opensearch.experimental
     */
    @ExperimentalApi
    public static final class StarTreeFieldType extends CompositeDataCubeFieldType {

        private final StarTreeFieldConfiguration starTreeConfig;

        public StarTreeFieldType(String name, StarTreeField starTreeField) {
            super(name, starTreeField.getDimensionsOrder(), starTreeField.getMetrics(), CompositeFieldType.STAR_TREE);
            this.starTreeConfig = starTreeField.getStarTreeConfig();
        }

        public StarTreeFieldConfiguration getStarTreeConfig() {
            return starTreeConfig;
        }

        @Override
        public ValueFetcher valueFetcher(QueryShardContext context, SearchLookup searchLookup, String format) {
            // TODO : evaluate later
            throw new UnsupportedOperationException("Cannot fetch values for star tree field [" + name() + "].");
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            // TODO : evaluate later
            throw new UnsupportedOperationException("Cannot perform terms query on star tree field [" + name() + "].");
        }
    }

}
