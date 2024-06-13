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
import org.opensearch.index.compositeindex.DateDimension;
import org.opensearch.index.compositeindex.Dimension;
import org.opensearch.index.compositeindex.Metric;
import org.opensearch.index.compositeindex.MetricType;
import org.opensearch.index.compositeindex.startree.StarTreeField;
import org.opensearch.index.compositeindex.startree.StarTreeFieldSpec;
import org.opensearch.index.compositeindex.startree.StarTreeIndexSettings;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.lookup.SearchLookup;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
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
        private final Parameter<StarTreeField> config = new Parameter<>("config", false, () -> null, (name, context, nodeObj) -> {
            if (nodeObj instanceof Map<?, ?>) {
                Map<String, Object> paramMap = (Map<String, Object>) nodeObj;
                int maxLeafDocs = XContentMapValues.nodeIntegerValue(
                    paramMap.get("max_leaf_docs"),
                    StarTreeIndexSettings.STAR_TREE_DEFAULT_MAX_LEAF_DOCS.get(context.getSettings())
                );
                if (maxLeafDocs < 1) {
                    throw new IllegalArgumentException(
                        String.format(Locale.ROOT, "max_leaf_docs [%s] must be greater than 0", maxLeafDocs)
                    );
                }
                List<String> skipStarInDims = Arrays.asList(
                    XContentMapValues.nodeStringArrayValue(
                        paramMap.getOrDefault("skip_star_node_creation_for_dimensions", new ArrayList<String>())
                    )
                );
                StarTreeFieldSpec.StarTreeBuildMode buildMode = StarTreeFieldSpec.StarTreeBuildMode.fromTypeName(
                    XContentMapValues.nodeStringValue(
                        paramMap.get("build_mode"),
                        StarTreeFieldSpec.StarTreeBuildMode.OFF_HEAP.getTypeName()
                    )
                );
                List<Dimension> dimensions = buildDimensions(paramMap, context);
                List<Metric> metrics = buildMetrics(paramMap, context);
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
                StarTreeFieldSpec spec = new StarTreeFieldSpec(maxLeafDocs, skipStarInDims, buildMode);
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
        private List<Dimension> buildDimensions(Map<String, Object> map, Mapper.TypeParser.ParserContext context) {
            Object dims = XContentMapValues.extractValue("ordered_dimensions", map);
            if (dims == null) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "ordered_dimensions is required for star tree field [%s]", this.name)
                );
            }
            List<Dimension> dimensions = new ArrayList<>();
            if (dims instanceof LinkedHashMap<?, ?>) {
                if (((LinkedHashMap<?, ?>) dims).size() > context.getSettings()
                    .getAsInt(StarTreeIndexSettings.STAR_TREE_MAX_DIMENSIONS_SETTING.getKey(), 10)) {
                    throw new IllegalArgumentException(
                        String.format(
                            Locale.ROOT,
                            "ordered_dimensions cannot have more than %s dimensions for star tree field [%s]",
                            context.getSettings().getAsInt(StarTreeIndexSettings.STAR_TREE_MAX_DIMENSIONS_SETTING.getKey(), 10),
                            this.name
                        )
                    );
                }
                if (((LinkedHashMap<?, ?>) dims).size() < 2) {
                    throw new IllegalArgumentException(
                        String.format(Locale.ROOT, "Atleast two dimensions are required to build star tree index field [%s]", this.name)
                    );
                }
                for (Map.Entry<String, Object> dim : ((LinkedHashMap<String, Object>) dims).entrySet()) {
                    if (this.objbuilder == null || this.objbuilder.mappersBuilders == null) {
                        if (dim.getValue() instanceof Map<?, ?>) {
                            Map<String, Object> dimObj = ((Map<String, Object>) dim.getValue());
                            String type = XContentMapValues.nodeStringValue(dimObj.get("type"));
                            dimensions.add(getDimension(type, dim, context));
                        } else {
                            throw new MapperParsingException(
                                String.format(Locale.ROOT, "unable to parse ordered_dimensions for star tree field [%s]", this.name)
                            );
                        }
                    } else {
                        Optional<Mapper.Builder> dimBuilder = findMapperBuilderByName(dim.getKey(), this.objbuilder.mappersBuilders);
                        if (dimBuilder.isEmpty()) {
                            throw new IllegalArgumentException(String.format(Locale.ROOT, "unknown dimension field [%s]", dim.getKey()));
                        }
                        if (!isBuilderAllowedForDimension(dimBuilder.get())) {
                            throw new IllegalArgumentException(
                                String.format(
                                    Locale.ROOT,
                                    "unsupported field type associated with dimension [%s] as part of star tree field [%s]",
                                    dim.getKey(),
                                    name
                                )
                            );
                        }
                        dimensions.add(getDimension(dimBuilder.get(), dim, context));
                    }
                }
            } else {
                throw new MapperParsingException(
                    String.format(Locale.ROOT, "unable to parse ordered_dimensions for star tree field [%s]", this.name)
                );
            }
            return dimensions;
        }

        /**
         * Get dimension instance based on the builder type
         */
        private Dimension getDimension(Mapper.Builder builder, Map.Entry<String, Object> dim, Mapper.TypeParser.ParserContext context) {
            String name = dim.getKey();
            Dimension dimension;
            if (builder instanceof DateFieldMapper.Builder) {
                dimension = new DateDimension(dim, context);
            }
            // Numeric dimension - default
            else if (builder instanceof NumberFieldMapper.Builder) {
                dimension = new Dimension(name);
            } else {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "unsupported field type associated with star tree dimension [%s]", name)
                );
            }
            return dimension;
        }

        /**
         * Get dimension based on field type
         */
        private Dimension getDimension(String type, Map.Entry<String, Object> dim, Mapper.TypeParser.ParserContext c) {
            String name = dim.getKey();
            Dimension dimension;
            if (type.equals("date")) {
                dimension = new DateDimension(dim, c);
            }
            // Numeric dimension - default
            else {
                dimension = new Dimension(name);
            }
            return dimension;
        }

        /**
         * Build metrics from mapping
         */
        @SuppressWarnings("unchecked")
        private List<Metric> buildMetrics(Map<String, Object> map, Mapper.TypeParser.ParserContext context) {
            List<Metric> metrics = new ArrayList<>();
            Object metricsFromInput = XContentMapValues.extractValue("metrics", map);
            if (metricsFromInput == null) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "metrics section is required for star tree field [%s]", this.name)
                );
            }
            if (metricsFromInput instanceof LinkedHashMap<?, ?>) {
                for (Map.Entry<String, Object> metric : ((LinkedHashMap<String, Object>) metricsFromInput).entrySet()) {
                    if (objbuilder == null || objbuilder.mappersBuilders == null) {
                        metrics.add(getMetric(metric, context));
                    } else {
                        Optional<Mapper.Builder> meticBuilder = findMapperBuilderByName(metric.getKey(), this.objbuilder.mappersBuilders);
                        if (meticBuilder.isEmpty()) {
                            throw new IllegalArgumentException(String.format(Locale.ROOT, "unknown metric field [%s]", metric.getKey()));
                        }
                        if (!isBuilderAllowedForMetric(meticBuilder.get())) {
                            throw new IllegalArgumentException(
                                String.format(Locale.ROOT, "non-numeric field type is associated with star tree metric [%s]", this.name)
                            );
                        }
                        metrics.add(getMetric(metric, context));
                    }
                }
            } else {
                throw new MapperParsingException(String.format(Locale.ROOT, "unable to parse metrics for star tree field [%s]", this.name));
            }

            return metrics;
        }

        @SuppressWarnings("unchecked")
        private Metric getMetric(Map.Entry<String, Object> map, Mapper.TypeParser.ParserContext context) {
            String name = map.getKey();
            List<MetricType> metricTypes;
            List<String> metricStrings = XContentMapValues.extractRawValues("metrics", (Map<String, Object>) map.getValue())
                .stream()
                .map(Object::toString)
                .collect(Collectors.toList());

            if (metricStrings.isEmpty()) {
                metricTypes = new ArrayList<>(StarTreeIndexSettings.DEFAULT_METRICS_LIST.get(context.getSettings()));
            } else {
                Set<MetricType> metricSet = new HashSet<>();
                for (String metricString : metricStrings) {
                    metricSet.add(MetricType.fromTypeName(metricString));
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

        private final StarTreeFieldSpec starTreeFieldSpec;

        public StarTreeFieldType(String name, StarTreeField starTreeField) {
            super(name, starTreeField.getDimensionsOrder(), starTreeField.getMetrics(), CompositeFieldType.STAR_TREE);
            this.starTreeFieldSpec = starTreeField.getSpec();
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
