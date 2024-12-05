/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.document.FieldType;
import org.opensearch.Version;
import org.opensearch.common.Explicit;
import org.opensearch.common.TriFunction;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.support.XContentMapValues;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.analysis.NamedAnalyzer;
import org.opensearch.index.mapper.Mapper.TypeParser.ParserContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Defines how a particular field should be indexed and searched
 * <p>
 * Configuration {@link Parameter}s for the mapper are defined on a {@link Builder} subclass,
 * and returned by its {@link Builder#getParameters()} method.  Merging, serialization
 * and parsing of the mapper are all mediated through this set of parameters.
 * <p>
 * Subclasses should implement a {@link Builder} that is returned from the
 * {@link #getMergeBuilder()} method, initialised with the existing builder.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public abstract class ParametrizedFieldMapper extends FieldMapper {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(ParametrizedFieldMapper.class);

    /**
     * Creates a new ParametrizedFieldMapper
     */
    protected ParametrizedFieldMapper(String simpleName, MappedFieldType mappedFieldType, MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, new FieldType(), mappedFieldType, multiFields, copyTo);
    }

    /**
     * Returns a {@link Builder} to be used for merging and serialization
     * <p>
     * Implement as follows:
     * {@code return new MyBuilder(simpleName()).init(this); }
     */
    public abstract ParametrizedFieldMapper.Builder getMergeBuilder();

    @Override
    public final ParametrizedFieldMapper merge(Mapper mergeWith) {

        if (mergeWith instanceof FieldMapper == false) {
            throw new IllegalArgumentException(
                "mapper ["
                    + name()
                    + "] cannot be changed from type ["
                    + contentType()
                    + "] to ["
                    + mergeWith.getClass().getSimpleName()
                    + "]"
            );
        }
        String mergeWithContentType = ((FieldMapper) mergeWith).contentType();
        if (Objects.equals(this.getClass(), mergeWith.getClass()) == false) {
            throw new IllegalArgumentException(
                "mapper [" + name() + "] cannot be changed from type [" + contentType() + "] to [" + mergeWithContentType + "]"
            );
        }
        if (Objects.equals(contentType(), mergeWithContentType) == false) {
            throw new IllegalArgumentException(
                "mapper [" + name() + "] cannot be changed from type [" + contentType() + "] to [" + mergeWithContentType + "]"
            );
        }

        ParametrizedFieldMapper.Builder builder = getMergeBuilder();
        if (builder == null) {
            return (ParametrizedFieldMapper) mergeWith;
        }
        Conflicts conflicts = new Conflicts(name());
        builder.merge((FieldMapper) mergeWith, conflicts);
        conflicts.check();
        return builder.build(new BuilderContext(Settings.EMPTY, parentPath(name())));
    }

    private static ContentPath parentPath(String name) {
        int endPos = name.lastIndexOf(".");
        if (endPos == -1) {
            return new ContentPath(0);
        }
        return new ContentPath(name.substring(0, endPos));
    }

    @Override
    protected final void mergeOptions(FieldMapper other, List<String> conflicts) {
        // TODO remove when everything is parametrized
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        builder.field("type", contentType());
        getMergeBuilder().toXContent(builder, includeDefaults);
        multiFields.toXContent(builder, params);
        copyTo.toXContent(builder, params);
    }

    /**
     * Serializes a parameter
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    protected interface Serializer<T> {
        void serialize(XContentBuilder builder, String name, T value) throws IOException;
    }

    /**
     * Check on whether or not a parameter should be serialized
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    protected interface SerializerCheck<T> {
        /**
         * Check on whether or not a parameter should be serialized
         * @param includeDefaults   if defaults have been requested
         * @param isConfigured      if the parameter has a different value to the default
         * @param value             the parameter value
         * @return {@code true} if the value should be serialized
         */
        boolean check(boolean includeDefaults, boolean isConfigured, T value);
    }

    /**
     * A configurable parameter for a field mapper
     * @param <T> the type of the value the parameter holds
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static final class Parameter<T> implements Supplier<T> {

        public final String name;
        private final List<String> deprecatedNames = new ArrayList<>();
        private final Supplier<T> defaultValue;
        private final TriFunction<String, ParserContext, Object, T> parser;
        private final Function<FieldMapper, T> initializer;
        private boolean acceptsNull = false;
        private Consumer<T> validator = null;
        private Serializer<T> serializer = XContentBuilder::field;
        private SerializerCheck<T> serializerCheck = (includeDefaults, isConfigured, value) -> includeDefaults || isConfigured;
        private Function<T, String> conflictSerializer = Objects::toString;
        private BiPredicate<T, T> mergeValidator;
        private T value;
        private boolean isSet;

        /**
         * Creates a new Parameter
         * @param name          the parameter name, used in parsing and serialization
         * @param updateable    whether the parameter can be updated with a new value during a mapping update
         * @param defaultValue  the default value for the parameter, used if unspecified in mappings
         * @param parser        a function that converts an object to a parameter value
         * @param initializer   a function that reads a parameter value from an existing mapper
         */
        public Parameter(
            String name,
            boolean updateable,
            Supplier<T> defaultValue,
            TriFunction<String, ParserContext, Object, T> parser,
            Function<FieldMapper, T> initializer
        ) {
            this.name = name;
            this.defaultValue = Objects.requireNonNull(defaultValue);
            this.value = null;
            this.parser = parser;
            this.initializer = initializer;
            this.mergeValidator = (previous, toMerge) -> updateable || Objects.equals(previous, toMerge);
        }

        /**
         * Returns the current value of the parameter
         */
        public T getValue() {
            return isSet ? value : defaultValue.get();
        }

        @Override
        public T get() {
            return getValue();
        }

        /**
         * Returns the default value of the parameter
         */
        public T getDefaultValue() {
            return defaultValue.get();
        }

        /**
         * Sets the current value of the parameter
         */
        public void setValue(T value) {
            this.isSet = true;
            this.value = value;
        }

        public boolean isConfigured() {
            return isSet && Objects.equals(value, defaultValue.get()) == false;
        }

        /**
         * Allows the parameter to accept a {@code null} value
         */
        public Parameter<T> acceptsNull() {
            this.acceptsNull = true;
            return this;
        }

        /**
         * Adds a deprecated parameter name.
         * <p>
         * If this parameter name is encountered during parsing, a deprecation warning will
         * be emitted.  The parameter will be serialized with its main name.
         */
        public Parameter<T> addDeprecatedName(String deprecatedName) {
            this.deprecatedNames.add(deprecatedName);
            return this;
        }

        /**
         * Adds validation to a parameter, called after parsing and merging
         */
        public Parameter<T> setValidator(Consumer<T> validator) {
            this.validator = validator;
            return this;
        }

        /**
         * Configure a custom serializer for this parameter
         */
        public Parameter<T> setSerializer(Serializer<T> serializer, Function<T, String> conflictSerializer) {
            this.serializer = serializer;
            this.conflictSerializer = conflictSerializer;
            return this;
        }

        /**
         * Configure a custom serialization check for this parameter
         */
        public Parameter<T> setSerializerCheck(SerializerCheck<T> check) {
            this.serializerCheck = check;
            return this;
        }

        /**
         * Always serialize this parameter, no matter its value
         */
        public Parameter<T> alwaysSerialize() {
            this.serializerCheck = (id, ic, v) -> true;
            return this;
        }

        /**
         * Never serialize this parameter, no matter its value
         */
        public Parameter<T> neverSerialize() {
            this.serializerCheck = (id, ic, v) -> false;
            return this;
        }

        /**
         * Sets a custom merge validator.  By default, merges are accepted if the
         * parameter is updateable, or if the previous and new values are equal
         */
        public Parameter<T> setMergeValidator(BiPredicate<T, T> mergeValidator) {
            this.mergeValidator = mergeValidator;
            return this;
        }

        private void validate() {
            if (validator != null) {
                validator.accept(getValue());
            }
        }

        private void init(FieldMapper toInit) {
            setValue(initializer.apply(toInit));
        }

        private void parse(String field, ParserContext context, Object in) {
            setValue(parser.apply(field, context, in));
        }

        private void merge(FieldMapper toMerge, Conflicts conflicts) {
            T value = initializer.apply(toMerge);
            T current = getValue();
            if (mergeValidator.test(current, value)) {
                setValue(value);
            } else {
                conflicts.addConflict(name, conflictSerializer.apply(current), conflictSerializer.apply(value));
            }
        }

        protected void toXContent(XContentBuilder builder, boolean includeDefaults) throws IOException {
            if (serializerCheck.check(includeDefaults, isConfigured(), get())) {
                serializer.serialize(builder, name, getValue());
            }
        }

        /**
         * Defines a parameter that takes the values {@code true} or {@code false}
         * @param name          the parameter name
         * @param updateable    whether the parameter can be changed by a mapping update
         * @param initializer   a function that reads the parameter value from an existing mapper
         * @param defaultValue  the default value, to be used if the parameter is undefined in a mapping
         */
        public static Parameter<Boolean> boolParam(
            String name,
            boolean updateable,
            Function<FieldMapper, Boolean> initializer,
            boolean defaultValue
        ) {
            return new Parameter<>(name, updateable, () -> defaultValue, (n, c, o) -> XContentMapValues.nodeBooleanValue(o), initializer);
        }

        /**
         * Defines a parameter that takes the values {@code true} or {@code false}, and will always serialize
         * its value if configured.
         * @param name          the parameter name
         * @param updateable    whether the parameter can be changed by a mapping update
         * @param initializer   a function that reads the parameter value from an existing mapper
         * @param defaultValue  the default value, to be used if the parameter is undefined in a mapping
         */
        public static Parameter<Explicit<Boolean>> explicitBoolParam(
            String name,
            boolean updateable,
            Function<FieldMapper, Explicit<Boolean>> initializer,
            boolean defaultValue
        ) {
            Explicit<Boolean> defaultExplicit = new Explicit<>(defaultValue, false);
            return new Parameter<>(
                name,
                updateable,
                () -> defaultExplicit,
                (n, c, o) -> new Explicit<>(XContentMapValues.nodeBooleanValue(o), true),
                initializer
            ).setSerializer((b, n, v) -> b.field(n, v.value()), v -> Boolean.toString(v.value()));
        }

        /**
         * Defines a parameter that takes a double value
         * @param name          the parameter name
         * @param updateable    whether the parameter can be changed by a mapping update
         * @param initializer   a function that reads the parameter value from an existing mapper
         * @param defaultValue  the default value, to be used if the parameter is undefined in a mapping
         */
        public static Parameter<Double> doubleParam(
            String name,
            boolean updateable,
            Function<FieldMapper, Double> initializer,
            double defaultValue
        ) {
            return new Parameter<>(name, updateable, () -> defaultValue, (n, c, o) -> XContentMapValues.nodeDoubleValue(o), initializer);
        }

        /**
         * Defines a parameter that takes a float value
         * @param name          the parameter name
         * @param updateable    whether the parameter can be changed by a mapping update
         * @param initializer   a function that reads the parameter value from an existing mapper
         * @param defaultValue  the default value, to be used if the parameter is undefined in a mapping
         */
        public static Parameter<Float> floatParam(
            String name,
            boolean updateable,
            Function<FieldMapper, Float> initializer,
            float defaultValue
        ) {
            return new Parameter<>(name, updateable, () -> defaultValue, (n, c, o) -> XContentMapValues.nodeFloatValue(o), initializer);
        }

        /**
         * Defines a parameter that takes an integer value
         * @param name          the parameter name
         * @param updateable    whether the parameter can be changed by a mapping update
         * @param initializer   a function that reads the parameter value from an existing mapper
         * @param defaultValue  the default value, to be used if the parameter is undefined in a mapping
         */
        public static Parameter<Integer> intParam(
            String name,
            boolean updateable,
            Function<FieldMapper, Integer> initializer,
            int defaultValue
        ) {
            return new Parameter<>(name, updateable, () -> defaultValue, (n, c, o) -> XContentMapValues.nodeIntegerValue(o), initializer);
        }

        /**
         * Defines a parameter that takes a string value
         * @param name          the parameter name
         * @param updateable    whether the parameter can be changed by a mapping update
         * @param initializer   a function that reads the parameter value from an existing mapper
         * @param defaultValue  the default value, to be used if the parameter is undefined in a mapping
         */
        public static Parameter<String> stringParam(
            String name,
            boolean updateable,
            Function<FieldMapper, String> initializer,
            String defaultValue
        ) {
            return new Parameter<>(name, updateable, () -> defaultValue, (n, c, o) -> XContentMapValues.nodeStringValue(o), initializer);
        }

        @SuppressWarnings("unchecked")
        public static Parameter<List<String>> stringArrayParam(
            String name,
            boolean updateable,
            Function<FieldMapper, List<String>> initializer,
            List<String> defaultValue
        ) {
            return new Parameter<>(name, updateable, () -> defaultValue, (n, c, o) -> {
                List<Object> values = (List<Object>) o;
                List<String> strValues = new ArrayList<>();
                for (Object item : values) {
                    strValues.add(item.toString());
                }
                return strValues;
            }, initializer);
        }

        /**
         * Defines a parameter that takes one of a restricted set of string values
         * @param name          the parameter name
         * @param updateable    whether the parameter can be changed by a mapping update
         * @param initializer   a function that reads the parameter value from an existing mapper
         * @param values        the set of values that the parameter can take.  The first value in the list
         *                      is the default value, to be used if the parameter is undefined in a mapping
         */
        public static Parameter<String> restrictedStringParam(
            String name,
            boolean updateable,
            Function<FieldMapper, String> initializer,
            String... values
        ) {
            assert values.length > 0;
            Set<String> acceptedValues = new LinkedHashSet<>(Arrays.asList(values));
            return stringParam(name, updateable, initializer, values[0]).setValidator(v -> {
                if (acceptedValues.contains(v)) {
                    return;
                }
                throw new MapperParsingException(
                    "Unknown value [" + v + "] for field [" + name + "] - accepted values are " + acceptedValues.toString()
                );
            });
        }

        /**
         * Defines a parameter that takes an analyzer name
         * @param name              the parameter name
         * @param updateable        whether the parameter can be changed by a mapping update
         * @param initializer       a function that reads the parameter value from an existing mapper
         * @param defaultAnalyzer   the default value, to be used if the parameter is undefined in a mapping
         */
        public static Parameter<NamedAnalyzer> analyzerParam(
            String name,
            boolean updateable,
            Function<FieldMapper, NamedAnalyzer> initializer,
            Supplier<NamedAnalyzer> defaultAnalyzer
        ) {
            return new Parameter<>(name, updateable, defaultAnalyzer, (n, c, o) -> {
                String analyzerName = o.toString();
                NamedAnalyzer a = c.getIndexAnalyzers().get(analyzerName);
                if (a == null) {
                    throw new IllegalArgumentException("analyzer [" + analyzerName + "] has not been configured in mappings");
                }
                return a;
            }, initializer).setSerializer((b, n, v) -> b.field(n, v.name()), NamedAnalyzer::name);
        }

        /**
         * Declares a metadata parameter
         */
        public static Parameter<Map<String, String>> metaParam() {
            return new Parameter<>(
                "meta",
                true,
                Collections::emptyMap,
                (n, c, o) -> TypeParsers.parseMeta(n, o),
                m -> m.fieldType().meta()
            );
        }

        public static Parameter<Boolean> indexParam(Function<FieldMapper, Boolean> initializer, boolean defaultValue) {
            return Parameter.boolParam("index", false, initializer, defaultValue);
        }

        public static Parameter<Boolean> storeParam(Function<FieldMapper, Boolean> initializer, boolean defaultValue) {
            return Parameter.boolParam("store", false, initializer, defaultValue);
        }

        public static Parameter<Boolean> docValuesParam(Function<FieldMapper, Boolean> initializer, boolean defaultValue) {
            return Parameter.boolParam("doc_values", false, initializer, defaultValue);
        }

        public static Parameter<Float> boostParam() {
            return Parameter.floatParam("boost", true, m -> m.fieldType().boost(), 1.0f);
        }

    }

    /**
     * Conflicts in the field mapper
     *
     * @opensearch.internal
     */
    private static final class Conflicts {

        private final String mapperName;
        private final List<String> conflicts = new ArrayList<>();

        Conflicts(String mapperName) {
            this.mapperName = mapperName;
        }

        void addConflict(String parameter, String existing, String toMerge) {
            conflicts.add("Cannot update parameter [" + parameter + "] from [" + existing + "] to [" + toMerge + "]");
        }

        void check() {
            if (conflicts.isEmpty()) {
                return;
            }
            String message = "Mapper for [" + mapperName + "] conflicts with existing mapper:\n\t" + String.join("\n\t", conflicts);
            throw new IllegalArgumentException(message);
        }

    }

    /**
     * A Builder for a ParametrizedFieldMapper
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public abstract static class Builder extends Mapper.Builder<Builder> {

        protected final MultiFields.Builder multiFieldsBuilder = new MultiFields.Builder();
        protected final CopyTo.Builder copyTo = new CopyTo.Builder();

        /**
         * Creates a new Builder with a field name
         */
        protected Builder(String name) {
            super(name);
        }

        /**
         * Initialises all parameters from an existing mapper
         */
        public Builder init(FieldMapper initializer) {
            for (Parameter<?> param : getParameters()) {
                param.init(initializer);
            }
            for (Mapper subField : initializer.multiFields) {
                multiFieldsBuilder.add(subField);
            }
            return this;
        }

        private void merge(FieldMapper in, Conflicts conflicts) {
            for (Parameter<?> param : getParameters()) {
                param.merge(in, conflicts);
            }
            for (Mapper newSubField : in.multiFields) {
                multiFieldsBuilder.update(newSubField, parentPath(newSubField.name()));
            }
            this.copyTo.reset(in.copyTo);
            validate();
        }

        private void validate() {
            for (Parameter<?> param : getParameters()) {
                param.validate();
            }
        }

        /**
         * @return the list of parameters defined for this mapper
         */
        protected abstract List<Parameter<?>> getParameters();

        @Override
        public abstract ParametrizedFieldMapper build(BuilderContext context);

        /**
         * Builds the full name of the field, taking into account parent objects
         */
        protected String buildFullName(BuilderContext context) {
            return context.path().pathAsText(name);
        }

        /**
         * Writes the current builder parameter values as XContent
         */
        protected final void toXContent(XContentBuilder builder, boolean includeDefaults) throws IOException {
            for (Parameter<?> parameter : getParameters()) {
                parameter.toXContent(builder, includeDefaults);
            }
        }

        /**
         * Parse mapping parameters from a map of mappings
         * @param name              the field mapper name
         * @param parserContext     the parser context
         * @param fieldNode         the root node of the map of mappings for this field
         */
        public final void parse(String name, ParserContext parserContext, Map<String, Object> fieldNode) {
            Map<String, Parameter<?>> paramsMap = new HashMap<>();
            Map<String, Parameter<?>> deprecatedParamsMap = new HashMap<>();
            for (Parameter<?> param : getParameters()) {
                paramsMap.put(param.name, param);
                for (String deprecatedName : param.deprecatedNames) {
                    deprecatedParamsMap.put(deprecatedName, param);
                }
            }
            String type = (String) fieldNode.get("type");
            if (paramsMap.get("type") == null) {
                fieldNode.remove("type");
            }

            for (Iterator<Map.Entry<String, Object>> iterator = fieldNode.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                final String propName = entry.getKey();
                final Object propNode = entry.getValue();
                if (Objects.equals("fields", propName)) {
                    TypeParsers.parseMultiField(multiFieldsBuilder::add, name, parserContext, propName, propNode);
                    iterator.remove();
                    continue;
                }
                if (Objects.equals("copy_to", propName)) {
                    TypeParsers.parseCopyFields(propNode).forEach(copyTo::add);
                    iterator.remove();
                    continue;
                }
                Parameter<?> parameter = deprecatedParamsMap.get(propName);
                if (parameter != null) {
                    deprecationLogger.deprecate(
                        propName + name,
                        "Parameter [{}] on mapper [{}] is deprecated, use [{}]",
                        propName,
                        name,
                        parameter.name
                    );
                } else {
                    parameter = paramsMap.get(propName);
                }
                if (parameter == null) {
                    if (isDeprecatedParameter(propName, parserContext.indexVersionCreated())) {
                        deprecationLogger.deprecate(
                            propName + type,
                            "Parameter [{}] has no effect on type [{}] and will be removed in future",
                            propName,
                            type
                        );
                        iterator.remove();
                        continue;
                    }
                    throw new MapperParsingException(
                        "unknown parameter [" + propName + "] on mapper [" + name + "] of type [" + type + "]"
                    );
                }
                if (Objects.equals("boost", propName)) {
                    deprecationLogger.deprecate(
                        "boost_" + name,
                        "Parameter [boost] on field [{}] is deprecated and will be removed in 3.0",
                        name
                    );
                }
                if (propNode == null && parameter.acceptsNull == false) {
                    throw new MapperParsingException(
                        "[" + propName + "] on mapper [" + name + "] of type [" + type + "] must not have a [null] value"
                    );
                }
                parameter.parse(name, parserContext, propNode);
                iterator.remove();
            }
            validate();
        }

        // These parameters were previously *always* parsed by TypeParsers#parseField(), even if they
        // made no sense; if we've got here, that means that they're not declared on a current mapper,
        // and so we emit a deprecation warning rather than failing a previously working mapping.
        private static final Set<String> DEPRECATED_PARAMS = new HashSet<>(
            Arrays.asList("store", "meta", "boost", "index", "doc_values", "index_options", "similarity")
        );

        private static boolean isDeprecatedParameter(String propName, Version indexCreatedVersion) {
            return DEPRECATED_PARAMS.contains(propName);
        }
    }

    /**
     * TypeParser implementation that automatically handles parsing
     *
     * @opensearch.internal
     */
    public static final class TypeParser implements Mapper.TypeParser {

        private final BiFunction<String, ParserContext, Builder> builderFunction;

        /**
         * Creates a new TypeParser
         * @param builderFunction a function that produces a Builder from a name and parsercontext
         */
        public TypeParser(BiFunction<String, ParserContext, Builder> builderFunction) {
            this.builderFunction = builderFunction;
        }

        @Override
        public Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            Builder builder = builderFunction.apply(name, parserContext);
            builder.parse(name, parserContext, node);
            return builder;
        }
    }
}
