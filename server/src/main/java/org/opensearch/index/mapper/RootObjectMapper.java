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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.opensearch.OpenSearchException;
import org.opensearch.common.Explicit;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.common.regex.Regex;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.mapper.DynamicTemplate.XContentFieldType;
import org.opensearch.index.mapper.MapperService.MergeReason;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

import static org.opensearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;
import static org.opensearch.index.mapper.TypeParsers.parseDateTimeFormatter;

/**
 * The root object mapper for a document
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class RootObjectMapper extends ObjectMapper {
    private static final DeprecationLogger DEPRECATION_LOGGER = DeprecationLogger.getLogger(RootObjectMapper.class);

    /**
     * Default parameters for root object
     *
     * @opensearch.internal
     */
    public static class Defaults {
        public static final DateFormatter[] DYNAMIC_DATE_TIME_FORMATTERS = new DateFormatter[] {
            DateFieldMapper.getDefaultDateTimeFormatter(),
            DateFormatter.forPattern("yyyy/MM/dd HH:mm:ss||yyyy/MM/dd||epoch_millis") };
        public static final boolean DATE_DETECTION = true;
        public static final boolean NUMERIC_DETECTION = false;
    }

    /**
     * Builder for the root object
     *
     * @opensearch.internal
     */
    public static class Builder extends ObjectMapper.Builder<Builder> {

        protected Explicit<DynamicTemplate[]> dynamicTemplates = new Explicit<>(new DynamicTemplate[0], false);
        protected Explicit<DynamicProperty[]> dynamicProperties = new Explicit<>(new DynamicProperty[0], false);
        protected Explicit<DateFormatter[]> dynamicDateTimeFormatters = new Explicit<>(Defaults.DYNAMIC_DATE_TIME_FORMATTERS, false);
        protected Explicit<Boolean> dateDetection = new Explicit<>(Defaults.DATE_DETECTION, false);
        protected Explicit<Boolean> numericDetection = new Explicit<>(Defaults.NUMERIC_DETECTION, false);

        public Builder(String name) {
            super(name);
            this.builder = this;
        }

        public Builder dynamicDateTimeFormatter(Collection<DateFormatter> dateTimeFormatters) {
            this.dynamicDateTimeFormatters = new Explicit<>(dateTimeFormatters.toArray(new DateFormatter[0]), true);
            return this;
        }

        public Builder dynamicTemplates(Collection<DynamicTemplate> templates) {
            this.dynamicTemplates = new Explicit<>(templates.toArray(new DynamicTemplate[0]), true);
            return this;
        }

        public Builder dynamicProperties(Collection<DynamicProperty> dynamicProperties) {
            this.dynamicProperties = new Explicit<>(dynamicProperties.toArray(new DynamicProperty[0]), true);
            return this;
        }

        @Override
        public RootObjectMapper build(BuilderContext context) {
            RootObjectMapper root = (RootObjectMapper) super.build(context);
            root.validateNoExplicitFieldMatchesDynamicProperty();
            return root;
        }

        @Override
        protected ObjectMapper createMapper(
            String name,
            String fullPath,
            Explicit<Boolean> enabled,
            Nested nested,
            Dynamic dynamic,
            Explicit<Boolean> disableObjects,
            Map<String, Mapper> mappers,
            @Nullable Settings settings
        ) {
            assert !nested.isNested();
            return new RootObjectMapper(
                name,
                enabled,
                dynamic,
                disableObjects,
                mappers,
                dynamicDateTimeFormatters,
                dynamicTemplates,
                dynamicProperties,
                dateDetection,
                numericDetection,
                settings
            );
        }
    }

    /**
     * Removes redundant root includes in {@link ObjectMapper.Nested} trees to avoid duplicate
     * fields on the root mapper when {@code isIncludeInRoot} is {@code true} for a node that is
     * itself included into a parent node, for which either {@code isIncludeInRoot} is
     * {@code true} or which is transitively included in root by a chain of nodes with
     * {@code isIncludeInParent} returning {@code true}.
     */
    public void fixRedundantIncludes() {
        fixRedundantIncludes(this, true);
    }

    private static void fixRedundantIncludes(ObjectMapper objectMapper, boolean parentIncluded) {
        for (Mapper mapper : objectMapper) {
            if (mapper instanceof ObjectMapper) {
                ObjectMapper child = (ObjectMapper) mapper;
                Nested nested = child.nested();
                boolean isNested = nested.isNested();
                boolean includeInRootViaParent = parentIncluded && isNested && nested.isIncludeInParent();
                boolean includedInRoot = isNested && nested.isIncludeInRoot();
                if (includeInRootViaParent && includedInRoot) {
                    nested.setIncludeInParent(true);
                    nested.setIncludeInRoot(false);
                }
                fixRedundantIncludes(child, includeInRootViaParent || includedInRoot);
            }
        }
    }

    /**
     * Type parser for the root object
     *
     * @opensearch.internal
     */
    public static class TypeParser extends ObjectMapper.TypeParser {

        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {

            RootObjectMapper.Builder builder = new Builder(name);
            Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator();
            Object compositeField = null;
            Object contextAwareGoupingField = null;
            while (iterator.hasNext()) {
                Map.Entry<String, Object> entry = iterator.next();
                String fieldName = entry.getKey();
                Object fieldNode = entry.getValue();
                if (fieldName.equals("composite")) {
                    compositeField = fieldNode;
                    iterator.remove();
                } else if (fieldName.equals(ContextAwareGroupingFieldMapper.CONTENT_TYPE)) {
                    contextAwareGoupingField = fieldNode;
                    iterator.remove();
                } else {
                    if (parseObjectOrDocumentTypeProperties(fieldName, fieldNode, parserContext, builder)
                        || processField(builder, fieldName, fieldNode, parserContext)) {
                        iterator.remove();
                    }
                }
            }
            // Important : Composite field is made up of 2 or more source properties of the index, so this must be called
            // after parsing all other properties
            if (compositeField != null) {
                parseCompositeField(builder, (Map<String, Object>) compositeField, parserContext);
            }

            if (contextAwareGoupingField != null) {
                parseContextAwareGroupingField(builder, (Map<String, Object>) contextAwareGoupingField, parserContext);
            }
            return builder;
        }

        protected static void parseContextAwareGroupingField(
            ObjectMapper.Builder objBuilder,
            Map<String, Object> contextAwareGroupingNode,
            ParserContext parserContext
        ) {
            Mapper.TypeParser typeParser = parserContext.typeParser(ContextAwareGroupingFieldMapper.CONTENT_TYPE);
            Mapper.Builder<?> mapperBuilder = typeParser.parse(
                ContextAwareGroupingFieldMapper.CONTENT_TYPE,
                contextAwareGroupingNode,
                parserContext,
                objBuilder
            );
            objBuilder.add(mapperBuilder);
        }

        protected boolean processField(RootObjectMapper.Builder builder, String fieldName, Object fieldNode, ParserContext parserContext) {
            if (fieldName.equals("date_formats") || fieldName.equals("dynamic_date_formats")) {
                if (fieldNode instanceof List) {
                    List<DateFormatter> formatters = new ArrayList<>();
                    for (Object formatter : (List<?>) fieldNode) {
                        if (formatter.toString().startsWith("epoch_")) {
                            throw new MapperParsingException("Epoch [" + formatter + "] is not supported as dynamic date format");
                        }
                        formatters.add(parseDateTimeFormatter(formatter));
                    }
                    builder.dynamicDateTimeFormatter(formatters);
                } else if ("none".equals(fieldNode.toString())) {
                    builder.dynamicDateTimeFormatter(Collections.emptyList());
                } else {
                    builder.dynamicDateTimeFormatter(Collections.singleton(parseDateTimeFormatter(fieldNode)));
                }
                return true;
            } else if (fieldName.equals("dynamic_templates")) {
                // "dynamic_templates" : [
                // {
                // "template_1" : {
                // "match" : "*_test",
                // "match_mapping_type" : "string",
                // "mapping" : { "type" : "keyword", "store" : "yes" }
                // }
                // }
                // ]
                if ((fieldNode instanceof List) == false) {
                    throw new MapperParsingException("Dynamic template syntax error. An array of named objects is expected.");
                }
                List<?> tmplNodes = (List<?>) fieldNode;
                List<DynamicTemplate> templates = new ArrayList<>();
                for (Object tmplNode : tmplNodes) {
                    Map<String, Object> tmpl = (Map<String, Object>) tmplNode;
                    if (tmpl.size() != 1) {
                        throw new MapperParsingException("A dynamic template must be defined with a name");
                    }
                    Map.Entry<String, Object> entry = tmpl.entrySet().iterator().next();
                    String templateName = entry.getKey();
                    Map<String, Object> templateParams = (Map<String, Object>) entry.getValue();
                    DynamicTemplate template = DynamicTemplate.parse(templateName, templateParams);
                    if (template != null) {
                        validateDynamicTemplate(parserContext, template);
                        templates.add(template);
                    }
                }
                builder.dynamicTemplates(templates);
                return true;
            } else if (fieldName.equals("dynamic_properties")) {
                if ((fieldNode instanceof Map) == false) {
                    throw new MapperParsingException(
                        "dynamic_properties must be an object with pattern keys and mapping objects as values."
                    );
                }
                @SuppressWarnings("unchecked")
                Map<String, Object> dynPropsNode = (Map<String, Object>) fieldNode;
                List<DynamicProperty> dynamicProperties = new ArrayList<>();
                for (Map.Entry<String, Object> entry : dynPropsNode.entrySet()) {
                    String pattern = entry.getKey();
                    if ("*".equals(pattern)) {
                        throw new MapperParsingException(
                            "dynamic_properties pattern [*] is not allowed; use a more specific pattern that still contains [*]. "
                                + "To apply one default mapping to all unknown field names, use dynamic_templates instead."
                        );
                    }
                    if (pattern.contains("*") == false) {
                        throw new MapperParsingException("dynamic_properties pattern [" + pattern + "] must contain a wildcard [*].");
                    }
                    if ((entry.getValue() instanceof Map) == false) {
                        throw new MapperParsingException("dynamic_properties entry [" + pattern + "] must have a mapping object as value.");
                    }
                    @SuppressWarnings("unchecked")
                    Map<String, Object> mapping = new TreeMap<>((Map<String, Object>) entry.getValue());
                    Object typeNode = mapping.get("type");
                    if (typeNode == null) {
                        throw new MapperParsingException("No type specified for dynamic_property pattern [" + pattern + "]");
                    }
                    String type = typeNode.toString();
                    Mapper.TypeParser typeParser = parserContext.typeParser(type);
                    if (typeParser == null) {
                        throw new MapperParsingException(
                            "No handler for type [" + type + "] declared for dynamic_property pattern [" + pattern + "]"
                        );
                    }
                    dynamicProperties.add(new DynamicProperty(pattern, mapping));
                }
                validateDynamicProperties(dynamicProperties);
                builder.dynamicProperties(dynamicProperties);
                return true;
            } else if (fieldName.equals("date_detection")) {
                builder.dateDetection = new Explicit<>(nodeBooleanValue(fieldNode, "date_detection"), true);
                return true;
            } else if (fieldName.equals("numeric_detection")) {
                builder.numericDetection = new Explicit<>(nodeBooleanValue(fieldNode, "numeric_detection"), true);
                return true;
            }
            return false;
        }
    }

    private Explicit<DateFormatter[]> dynamicDateTimeFormatters;
    private Explicit<Boolean> dateDetection;
    private Explicit<Boolean> numericDetection;
    private Explicit<DynamicTemplate[]> dynamicTemplates;
    private Explicit<DynamicProperty[]> dynamicProperties;

    RootObjectMapper(
        String name,
        Explicit<Boolean> enabled,
        Dynamic dynamic,
        Explicit<Boolean> disableObjects,
        Map<String, Mapper> mappers,
        Explicit<DateFormatter[]> dynamicDateTimeFormatters,
        Explicit<DynamicTemplate[]> dynamicTemplates,
        Explicit<DynamicProperty[]> dynamicProperties,
        Explicit<Boolean> dateDetection,
        Explicit<Boolean> numericDetection,
        Settings settings
    ) {
        super(name, name, enabled, Nested.NO, dynamic, disableObjects, mappers, settings);
        this.dynamicTemplates = dynamicTemplates;
        this.dynamicDateTimeFormatters = dynamicDateTimeFormatters;
        this.dynamicProperties = dynamicProperties;
        this.dateDetection = dateDetection;
        this.numericDetection = numericDetection;
    }

    @Override
    public ObjectMapper mappingUpdate(Mapper mapper) {
        RootObjectMapper update = (RootObjectMapper) super.mappingUpdate(mapper);
        // for dynamic updates, no need to carry root-specific options, we just
        // set everything to they implicit default value so that they are not
        // applied at merge time
        update.dynamicTemplates = new Explicit<>(new DynamicTemplate[0], false);
        update.dynamicProperties = new Explicit<>(new DynamicProperty[0], false);
        update.dynamicDateTimeFormatters = new Explicit<>(Defaults.DYNAMIC_DATE_TIME_FORMATTERS, false);
        update.dateDetection = new Explicit<>(Defaults.DATE_DETECTION, false);
        update.numericDetection = new Explicit<>(Defaults.NUMERIC_DETECTION, false);
        return update;
    }

    public boolean dateDetection() {
        return this.dateDetection.value();
    }

    public boolean numericDetection() {
        return this.numericDetection.value();
    }

    public DateFormatter[] dynamicDateTimeFormatters() {
        return dynamicDateTimeFormatters.value();
    }

    public DynamicTemplate[] dynamicTemplates() {
        return dynamicTemplates.value();
    }

    public DynamicProperty[] dynamicProperties() {
        return dynamicProperties.value();
    }

    /**
     * Returns the first dynamic property that matches the given field name, or null.
     * Patterns are checked in declaration / merge order (first match wins).
     */
    public DynamicProperty findDynamicProperty(String fullFieldName) {
        for (DynamicProperty dp : dynamicProperties.value()) {
            if (dp.matches(fullFieldName)) {
                return dp;
            }
        }
        return null;
    }

    @SuppressWarnings("rawtypes")
    public Mapper.Builder findTemplateBuilder(ParseContext context, String name, XContentFieldType matchType) {
        return findTemplateBuilder(context, name, matchType, null);
    }

    public Mapper.Builder findTemplateBuilder(ParseContext context, String name, DateFormatter dateFormatter) {
        return findTemplateBuilder(context, name, XContentFieldType.DATE, dateFormatter);
    }

    /**
     * Find a template. Returns {@code null} if no template could be found.
     * @param name        the field name
     * @param matchType   the type of the field in the json document or null if unknown
     * @param dateFormat  a dateformatter to use if the type is a date, null if not a date or is using the default format
     * @return a mapper builder, or null if there is no template for such a field
     */
    @SuppressWarnings("rawtypes")
    private Mapper.Builder findTemplateBuilder(ParseContext context, String name, XContentFieldType matchType, DateFormatter dateFormat) {
        DynamicTemplate dynamicTemplate = findTemplate(context.path(), name, matchType);
        if (dynamicTemplate == null) {
            return null;
        }
        String dynamicType = matchType.defaultMappingType();
        Mapper.TypeParser.ParserContext parserContext = context.docMapperParser().parserContext(dateFormat);
        String mappingType = dynamicTemplate.mappingType(dynamicType);
        Mapper.TypeParser typeParser = parserContext.typeParser(mappingType);
        if (typeParser == null) {
            throw new MapperParsingException("failed to find type parsed [" + mappingType + "] for [" + name + "]");
        }
        return typeParser.parse(name, dynamicTemplate.mappingForName(name, dynamicType), parserContext);
    }

    public DynamicTemplate findTemplate(ContentPath path, String name, XContentFieldType matchType) {
        final String pathAsString = path.pathAsText(name);
        for (DynamicTemplate dynamicTemplate : dynamicTemplates.value()) {
            if (dynamicTemplate.match(pathAsString, name, matchType)) {
                return dynamicTemplate;
            }
        }
        return null;
    }

    @Override
    public RootObjectMapper merge(Mapper mergeWith, MergeReason reason) {
        return (RootObjectMapper) super.merge(mergeWith, reason);
    }

    @Override
    protected void doMerge(ObjectMapper mergeWith, MergeReason reason) {
        super.doMerge(mergeWith, reason);
        RootObjectMapper mergeWithObject = (RootObjectMapper) mergeWith;
        if (mergeWithObject.numericDetection.explicit()) {
            this.numericDetection = mergeWithObject.numericDetection;
        }

        if (mergeWithObject.dateDetection.explicit()) {
            this.dateDetection = mergeWithObject.dateDetection;
        }

        if (mergeWithObject.dynamicDateTimeFormatters.explicit()) {
            this.dynamicDateTimeFormatters = mergeWithObject.dynamicDateTimeFormatters;
        }

        if (mergeWithObject.dynamicTemplates.explicit()) {
            if (reason == MergeReason.INDEX_TEMPLATE) {
                Map<String, DynamicTemplate> templatesByKey = new LinkedHashMap<>();
                for (DynamicTemplate template : this.dynamicTemplates.value()) {
                    templatesByKey.put(template.name(), template);
                }
                for (DynamicTemplate template : mergeWithObject.dynamicTemplates.value()) {
                    templatesByKey.put(template.name(), template);
                }

                DynamicTemplate[] mergedTemplates = templatesByKey.values().toArray(new DynamicTemplate[0]);
                this.dynamicTemplates = new Explicit<>(mergedTemplates, true);
            } else {
                this.dynamicTemplates = mergeWithObject.dynamicTemplates;
            }
        }

        if (mergeWithObject.dynamicProperties.explicit()) {
            // Merge by pattern key for live mapping updates and templates so a partial PUT does not drop
            // existing patterns (same as INDEX_TEMPLATE). Recovery applies persisted mapping as-is.
            if (reason == MergeReason.MAPPING_RECOVERY) {
                this.dynamicProperties = mergeWithObject.dynamicProperties;
            } else {
                Map<String, DynamicProperty> byPattern = new LinkedHashMap<>();
                for (DynamicProperty dp : this.dynamicProperties.value()) {
                    byPattern.put(dp.getPattern(), dp);
                }
                for (DynamicProperty dp : mergeWithObject.dynamicProperties.value()) {
                    byPattern.put(dp.getPattern(), dp);
                }
                List<DynamicProperty> merged = new ArrayList<>(byPattern.values());
                validateDynamicProperties(merged);
                this.dynamicProperties = new Explicit<>(merged.toArray(new DynamicProperty[0]), true);
            }
        }
        if (reason != MergeReason.MAPPING_RECOVERY) {
            validateNoExplicitFieldMatchesDynamicProperty();
        }
    }

    @Override
    protected void doXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        final boolean includeDefaults = params.paramAsBoolean("include_defaults", false);

        if (dynamicDateTimeFormatters.explicit() || includeDefaults) {
            builder.startArray("dynamic_date_formats");
            for (DateFormatter dateTimeFormatter : dynamicDateTimeFormatters.value()) {
                builder.value(dateTimeFormatter.pattern());
            }
            builder.endArray();
        }

        if (dynamicTemplates.explicit() || includeDefaults) {
            builder.startArray("dynamic_templates");
            for (DynamicTemplate dynamicTemplate : dynamicTemplates.value()) {
                builder.startObject();
                builder.field(dynamicTemplate.name(), dynamicTemplate);
                builder.endObject();
            }
            builder.endArray();
        }

        if (dynamicProperties.explicit() || includeDefaults) {
            builder.startObject("dynamic_properties");
            for (DynamicProperty dp : dynamicProperties.value()) {
                builder.field(dp.getPattern(), dp.getMapping());
            }
            builder.endObject();
        }

        if (dateDetection.explicit() || includeDefaults) {
            builder.field("date_detection", dateDetection.value());
        }
        if (numericDetection.explicit() || includeDefaults) {
            builder.field("numeric_detection", numericDetection.value());
        }
    }

    /**
     * Validates that no explicitly mapped field path is matched by any dynamic_property pattern.
     * Per the dynamic_properties contract, explicit properties cannot overlap with dynamic patterns.
     * Paths use each mapper's {@link Mapper#name()}, which is already the full dotted path from the
     * mapping root (same as field names in queries), not simpleName + manual prefix concatenation.
     */
    private void validateNoExplicitFieldMatchesDynamicProperty() {
        if (dynamicProperties.value().length == 0) {
            return;
        }
        List<String> explicitPaths = new ArrayList<>();
        collectFullPaths(this, explicitPaths);
        for (String path : explicitPaths) {
            if (path.isEmpty()) {
                continue;
            }
            for (DynamicProperty dp : dynamicProperties.value()) {
                if (dp.matches(path)) {
                    throw new MapperParsingException(
                        "Cannot add explicit property ["
                            + path
                            + "] because it is matched by dynamic_property pattern ["
                            + dp.getPattern()
                            + "]. Explicit fields cannot overlap with dynamic_properties."
                    );
                }
            }
        }
    }

    private static void collectFullPaths(ObjectMapper obj, List<String> paths) {
        for (Mapper mapper : obj) {
            // ObjectMapper.name() and FieldMapper.name() both return the full path (e.g. parent.child),
            // not a leaf segment; prefix + name() would incorrectly produce parent.parent.child.
            paths.add(mapper.name());
            if (mapper instanceof ObjectMapper child) {
                collectFullPaths(child, paths);
            }
        }
    }

    /**
     * Maximum number of {@code dynamic_properties} patterns allowed in a single mapping. Kept low by default
     * because overlap detection is O(n²) in the number of patterns; raising this limit also raises that cost.
     */
    public static final int MAX_DYNAMIC_PROPERTIES = 50;

    /**
     * Validates {@code dynamic_properties}. The bare pattern {@code *} is rejected at parse time; overlap
     * between any two remaining patterns is not allowed (automaton intersection). The total number of patterns
     * is also capped at {@link #MAX_DYNAMIC_PROPERTIES} to bound the O(n²) overlap-detection cost.
     */
    private static void validateDynamicProperties(List<DynamicProperty> dynamicProperties) {
        if (dynamicProperties.isEmpty()) {
            return;
        }
        if (dynamicProperties.size() > MAX_DYNAMIC_PROPERTIES) {
            throw new MapperParsingException(
                "The number of dynamic_properties patterns ["
                    + dynamicProperties.size()
                    + "] exceeds the maximum allowed ["
                    + MAX_DYNAMIC_PROPERTIES
                    + "]."
            );
        }
        // Reject any pair of patterns whose glob languages overlap (same field name can match both).
        // Uses the same automaton construction as Regex.simpleMatch / Glob.globMatch (see Regex.simpleMatchToAutomaton).
        for (int i = 0; i < dynamicProperties.size(); i++) {
            DynamicProperty p1 = dynamicProperties.get(i);
            for (int j = i + 1; j < dynamicProperties.size(); j++) {
                DynamicProperty p2 = dynamicProperties.get(j);
                if (dynamicPropertyPatternsOverlap(p1.getPattern(), p2.getPattern())) {
                    throw new MapperParsingException(
                        "dynamic_properties patterns ["
                            + p1.getPattern()
                            + "] and ["
                            + p2.getPattern()
                            + "] overlap (both can match the same field name)."
                    );
                }
            }
        }
    }

    /**
     * True iff some field name matches both glob patterns (same semantics as {@link Regex#simpleMatch}).
     */
    private static boolean dynamicPropertyPatternsOverlap(String pattern1, String pattern2) {
        Automaton a1 = Regex.simpleMatchToAutomaton(pattern1);
        Automaton a2 = Regex.simpleMatchToAutomaton(pattern2);
        return Operations.isEmpty(Operations.intersection(a1, a2)) == false;
    }

    private static void validateDynamicTemplate(Mapper.TypeParser.ParserContext parserContext, DynamicTemplate dynamicTemplate) {

        if (containsSnippet(dynamicTemplate.getMapping(), "{name}")) {
            // Can't validate template, because field names can't be guessed up front.
            return;
        }

        final XContentFieldType[] types;
        if (dynamicTemplate.getXContentFieldType() != null) {
            types = new XContentFieldType[] { dynamicTemplate.getXContentFieldType() };
        } else {
            types = XContentFieldType.values();
        }

        Exception lastError = null;
        boolean dynamicTemplateInvalid = true;

        for (XContentFieldType contentFieldType : types) {
            String defaultDynamicType = contentFieldType.defaultMappingType();
            String mappingType = dynamicTemplate.mappingType(defaultDynamicType);
            Mapper.TypeParser typeParser = parserContext.typeParser(mappingType);
            if (typeParser == null) {
                lastError = new IllegalArgumentException("No mapper found for type [" + mappingType + "]");
                continue;
            }

            String templateName = "__dynamic__" + dynamicTemplate.name();
            Map<String, Object> fieldTypeConfig = dynamicTemplate.mappingForName(templateName, defaultDynamicType);
            try {
                Mapper.Builder<?> dummyBuilder = typeParser.parse(templateName, fieldTypeConfig, parserContext);
                fieldTypeConfig.remove("type");
                if (fieldTypeConfig.isEmpty()) {
                    Settings indexSettings = parserContext.mapperService().getIndexSettings().getSettings();
                    BuilderContext builderContext = new BuilderContext(indexSettings, new ContentPath(1));
                    dummyBuilder.build(builderContext);
                    dynamicTemplateInvalid = false;
                    break;
                } else {
                    lastError = new IllegalArgumentException("Unused mapping attributes [" + fieldTypeConfig + "]");
                }
            } catch (Exception e) {
                lastError = e;
            }
        }

        if (dynamicTemplateInvalid) {
            String message = String.format(
                Locale.ROOT,
                "dynamic template [%s] has invalid content [%s]",
                dynamicTemplate.getName(),
                Strings.toString(MediaTypeRegistry.JSON, dynamicTemplate)
            );

            final String deprecationMessage;
            if (lastError != null) {
                deprecationMessage = String.format(Locale.ROOT, "%s, caused by [%s]", message, lastError.getMessage());
            } else {
                deprecationMessage = message;
            }
            DEPRECATION_LOGGER.deprecate("invalid_dynamic_template_" + dynamicTemplate.getName(), deprecationMessage);
        }
    }

    private static boolean containsSnippet(Map<?, ?> map, String snippet) {
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            String key = entry.getKey().toString();
            if (key.contains(snippet)) {
                return true;
            }

            Object value = entry.getValue();
            if (value instanceof Map) {
                if (containsSnippet((Map<?, ?>) value, snippet)) {
                    return true;
                }
            } else if (value instanceof List) {
                if (containsSnippet((List<?>) value, snippet)) {
                    return true;
                }
            } else if (value instanceof String) {
                String valueString = (String) value;
                if (valueString.contains(snippet)) {
                    return true;
                }
            }
        }

        return false;
    }

    private static boolean containsSnippet(List<?> list, String snippet) {
        for (Object value : list) {
            if (value instanceof Map) {
                if (containsSnippet((Map<?, ?>) value, snippet)) {
                    return true;
                }
            } else if (value instanceof List) {
                if (containsSnippet((List<?>) value, snippet)) {
                    return true;
                }
            } else if (value instanceof String) {
                String valueString = (String) value;
                if (valueString.contains(snippet)) {
                    return true;
                }
            }
        }
        return false;
    }

    public BytesReference deriveSource(LeafReader leafReader, int docId) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        try {
            Iterator<Mapper> mappers = this.iterator();
            while (mappers.hasNext()) {
                Mapper mapper = mappers.next();
                mapper.deriveSource(builder, leafReader, docId);
            }
        } catch (Exception e) {
            throw new OpenSearchException("Failed to derive source for doc id [" + docId + "]", e);
        } finally {
            builder.endObject();
        }
        return BytesReference.bytes(builder);
    }
}
