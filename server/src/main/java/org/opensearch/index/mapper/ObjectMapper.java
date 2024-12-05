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

import org.apache.lucene.search.Query;
import org.opensearch.OpenSearchParseException;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.Explicit;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.collect.CopyOnWriteHashMap;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.xcontent.support.XContentMapValues;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeIndexSettings;
import org.opensearch.index.mapper.MapperService.MergeReason;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Field mapper for object field types
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class ObjectMapper extends Mapper implements Cloneable {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(ObjectMapper.class);

    public static final String CONTENT_TYPE = "object";
    public static final String NESTED_CONTENT_TYPE = "nested";

    /**
     * Parameter defaults
     *
     * @opensearch.internal
     */
    public static class Defaults {
        public static final boolean ENABLED = true;
        public static final Nested NESTED = Nested.NO;
        public static final Dynamic DYNAMIC = null; // not set, inherited from root
    }

    /**
     * Dynamic field mapping specification
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public enum Dynamic {
        TRUE,
        FALSE,
        STRICT,
        STRICT_ALLOW_TEMPLATES
    }

    /**
     * Nested objects
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class Nested {

        public static final Nested NO = new Nested(false, new Explicit<>(false, false), new Explicit<>(false, false));

        public static Nested newNested() {
            return new Nested(true, new Explicit<>(false, false), new Explicit<>(false, false));
        }

        public static Nested newNested(Explicit<Boolean> includeInParent, Explicit<Boolean> includeInRoot) {
            return new Nested(true, includeInParent, includeInRoot);
        }

        private final boolean nested;
        private Explicit<Boolean> includeInParent;
        private Explicit<Boolean> includeInRoot;

        private Nested(boolean nested, Explicit<Boolean> includeInParent, Explicit<Boolean> includeInRoot) {
            this.nested = nested;
            this.includeInParent = includeInParent;
            this.includeInRoot = includeInRoot;
        }

        public void merge(Nested mergeWith, MergeReason reason) {
            if (isNested()) {
                if (!mergeWith.isNested()) {
                    throw new IllegalArgumentException("cannot change object mapping from nested to non-nested");
                }
            } else {
                if (mergeWith.isNested()) {
                    throw new IllegalArgumentException("cannot change object mapping from non-nested to nested");
                }
            }

            if (reason == MergeReason.INDEX_TEMPLATE) {
                if (mergeWith.includeInParent.explicit()) {
                    includeInParent = mergeWith.includeInParent;
                }
                if (mergeWith.includeInRoot.explicit()) {
                    includeInRoot = mergeWith.includeInRoot;
                }
            } else {
                if (includeInParent.value() != mergeWith.includeInParent.value()) {
                    throw new MapperException("the [include_in_parent] parameter can't be updated on a nested object mapping");
                }
                if (includeInRoot.value() != mergeWith.includeInRoot.value()) {
                    throw new MapperException("the [include_in_root] parameter can't be updated on a nested object mapping");
                }
            }
        }

        public boolean isNested() {
            return nested;
        }

        public boolean isIncludeInParent() {
            return includeInParent.value();
        }

        public boolean isIncludeInRoot() {
            return includeInRoot.value();
        }

        public void setIncludeInParent(boolean value) {
            includeInParent = new Explicit<>(value, true);
        }

        public void setIncludeInRoot(boolean value) {
            includeInRoot = new Explicit<>(value, true);
        }

        public static boolean isParent(ObjectMapper parentObjectMapper, ObjectMapper childObjectMapper, MapperService mapperService) {
            if (parentObjectMapper == null || childObjectMapper == null) {
                return false;
            }

            ObjectMapper parent = childObjectMapper.getParentObjectMapper(mapperService);
            while (parent != null && parent != parentObjectMapper) {
                childObjectMapper = parent;
                parent = childObjectMapper.getParentObjectMapper(mapperService);
            }
            return parentObjectMapper == parent;
        }
    }

    /**
     * Builder for object field mapper
     *
     * @opensearch.internal
     */
    @SuppressWarnings("rawtypes")
    @PublicApi(since = "1.0.0")
    public static class Builder<T extends Builder> extends Mapper.Builder<T> {

        protected Explicit<Boolean> enabled = new Explicit<>(true, false);

        protected Nested nested = Defaults.NESTED;

        protected Dynamic dynamic = Defaults.DYNAMIC;

        protected final List<Mapper.Builder> mappersBuilders = new ArrayList<>();

        public Builder(String name) {
            super(name);
            this.builder = (T) this;
        }

        public T enabled(boolean enabled) {
            this.enabled = new Explicit<>(enabled, true);
            return builder;
        }

        public T dynamic(Dynamic dynamic) {
            this.dynamic = dynamic;
            return builder;
        }

        public T nested(Nested nested) {
            this.nested = nested;
            return builder;
        }

        public T add(Mapper.Builder builder) {
            mappersBuilders.add(builder);
            return this.builder;
        }

        @Override
        public ObjectMapper build(BuilderContext context) {
            context.path().add(name);

            Map<String, Mapper> mappers = new HashMap<>();
            for (Mapper.Builder builder : mappersBuilders) {
                Mapper mapper = builder.build(context);
                Mapper existing = mappers.get(mapper.simpleName());
                if (existing != null) {
                    mapper = existing.merge(mapper);
                }
                mappers.put(mapper.simpleName(), mapper);
            }
            context.path().remove();

            ObjectMapper objectMapper = createMapper(
                name,
                context.path().pathAsText(name),
                enabled,
                nested,
                dynamic,
                mappers,
                context.indexSettings()
            );

            return objectMapper;
        }

        protected ObjectMapper createMapper(
            String name,
            String fullPath,
            Explicit<Boolean> enabled,
            Nested nested,
            Dynamic dynamic,
            Map<String, Mapper> mappers,
            @Nullable Settings settings
        ) {
            return new ObjectMapper(name, fullPath, enabled, nested, dynamic, mappers, settings);
        }
    }

    /**
     * Type parser for object field mapper
     *
     * @opensearch.internal
     */
    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            ObjectMapper.Builder builder = new Builder(name);
            parseNested(name, node, builder, parserContext);
            Object compositeField = null;
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String fieldName = entry.getKey();
                Object fieldNode = entry.getValue();
                if (fieldName.equals("composite")) {
                    compositeField = fieldNode;
                    iterator.remove();
                } else {
                    if (parseObjectOrDocumentTypeProperties(fieldName, fieldNode, parserContext, builder)) {
                        iterator.remove();
                    }
                }
            }
            // Important : Composite field is made up of 2 or more source fields of the index, so this must be called
            // after parsing all other properties
            if (compositeField != null) {
                parseCompositeField(builder, (Map<String, Object>) compositeField, parserContext);
            }
            return builder;
        }

        protected static boolean parseObjectOrDocumentTypeProperties(
            String fieldName,
            Object fieldNode,
            ParserContext parserContext,
            ObjectMapper.Builder builder
        ) {
            if (fieldName.equals("dynamic")) {
                String value = fieldNode.toString();
                if (value.equalsIgnoreCase("strict")) {
                    builder.dynamic(Dynamic.STRICT);
                } else if (value.equalsIgnoreCase("strict_allow_templates")) {
                    builder.dynamic(Dynamic.STRICT_ALLOW_TEMPLATES);
                } else {
                    boolean dynamic = XContentMapValues.nodeBooleanValue(fieldNode, fieldName + ".dynamic");
                    builder.dynamic(dynamic ? Dynamic.TRUE : Dynamic.FALSE);
                }
                return true;
            } else if (fieldName.equals("enabled")) {
                builder.enabled(XContentMapValues.nodeBooleanValue(fieldNode, fieldName + ".enabled"));
                return true;
            } else if (fieldName.equals("derived")) {
                if (fieldNode instanceof Collection && ((Collection) fieldNode).isEmpty()) {
                    // nothing to do here, empty (to support "derived: []" case)
                } else if (fieldNode instanceof Map) {
                    parseDerived(builder, (Map<String, Object>) fieldNode, parserContext);
                } else {
                    throw new OpenSearchParseException("derived must be a map type");
                }
                return true;
            } else if (fieldName.equals("properties")) {
                if (fieldNode instanceof Collection && ((Collection) fieldNode).isEmpty()) {
                    // nothing to do here, empty (to support "properties: []" case)
                } else if (!(fieldNode instanceof Map)) {
                    throw new OpenSearchParseException("properties must be a map type");
                } else {
                    parseProperties(builder, (Map<String, Object>) fieldNode, parserContext);
                }
                return true;
            } else if (fieldName.equals("include_in_all")) {
                deprecationLogger.deprecate(
                    "include_in_all",
                    "[include_in_all] is deprecated, the _all field have been removed in this version"
                );
                return true;
            }
            return false;
        }

        protected static void parseNested(
            String name,
            Map<String, Object> node,
            ObjectMapper.Builder builder,
            ParserContext parserContext
        ) {
            boolean nested = false;
            Explicit<Boolean> nestedIncludeInParent = new Explicit<>(false, false);
            Explicit<Boolean> nestedIncludeInRoot = new Explicit<>(false, false);
            Object fieldNode = node.get("type");
            if (fieldNode != null) {
                String type = fieldNode.toString();
                if (type.equals(CONTENT_TYPE)) {
                    builder.nested = Nested.NO;
                } else if (type.equals(NESTED_CONTENT_TYPE)) {
                    nested = true;
                } else {
                    throw new MapperParsingException(
                        "Trying to parse an object but has a different type [" + type + "] for [" + name + "]"
                    );
                }
            }
            fieldNode = node.get("include_in_parent");
            if (fieldNode != null) {
                boolean includeInParent = XContentMapValues.nodeBooleanValue(fieldNode, name + ".include_in_parent");
                nestedIncludeInParent = new Explicit<>(includeInParent, true);
                node.remove("include_in_parent");
            }
            fieldNode = node.get("include_in_root");
            if (fieldNode != null) {
                boolean includeInRoot = XContentMapValues.nodeBooleanValue(fieldNode, name + ".include_in_root");
                nestedIncludeInRoot = new Explicit<>(includeInRoot, true);
                node.remove("include_in_root");
            }
            if (nested) {
                builder.nested = Nested.newNested(nestedIncludeInParent, nestedIncludeInRoot);
            }
        }

        protected static void parseDerived(ObjectMapper.Builder objBuilder, Map<String, Object> derivedNode, ParserContext parserContext) {
            Iterator<Map.Entry<String, Object>> iterator = derivedNode.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, Object> entry = iterator.next();
                String fieldName = entry.getKey();
                // Should accept empty arrays, as a work around for when the
                // user can't provide an empty Map. (PHP for example)
                boolean isEmptyList = entry.getValue() instanceof List && ((List<?>) entry.getValue()).isEmpty();

                if (entry.getValue() instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> node = (Map<String, Object>) entry.getValue();

                    // Derived fields are a bit unique in that the 'type' attribute does not map to the TypeParser
                    // like it would for traditional fields in properties.
                    // So in this case, the DerivedFieldMapper's TypeParser will explicitly be used
                    Mapper.TypeParser typeParser = parserContext.typeParser(DerivedFieldMapper.CONTENT_TYPE);
                    String[] fieldNameParts = fieldName.split("\\.");
                    // field name is just ".", which is invalid
                    if (fieldNameParts.length < 1) {
                        throw new MapperParsingException("Invalid field name " + fieldName);
                    }
                    String realFieldName = fieldNameParts[fieldNameParts.length - 1];
                    Mapper.Builder<?> fieldBuilder = typeParser.parse(realFieldName, node, parserContext);
                    for (int i = fieldNameParts.length - 2; i >= 0; --i) {
                        ObjectMapper.Builder<?> intermediate = new ObjectMapper.Builder<>(fieldNameParts[i]);
                        intermediate.add(fieldBuilder);
                        fieldBuilder = intermediate;
                    }
                    objBuilder.add(fieldBuilder);
                    node.remove("type");
                    DocumentMapperParser.checkNoRemainingFields(fieldName, node, parserContext.indexVersionCreated());
                    iterator.remove();
                } else if (isEmptyList) {
                    iterator.remove();
                } else {
                    throw new MapperParsingException(
                        "Expected map for property [derived_fields] on field [" + fieldName + "] but got a " + fieldName.getClass()
                    );
                }
            }

            DocumentMapperParser.checkNoRemainingFields(
                derivedNode,
                parserContext.indexVersionCreated(),
                "DocType mapping definition has unsupported parameters: "
            );
        }

        /**
         * Contains logic to parse 'composite' section of mappings. This should be called only after
         * parsing normal properties as this checks if the given fields are also source fields of the index.
         */
        protected static void parseCompositeField(
            ObjectMapper.Builder objBuilder,
            Map<String, Object> compositeNode,
            ParserContext parserContext
        ) {
            if (!FeatureFlags.isEnabled(FeatureFlags.STAR_TREE_INDEX_SETTING)) {
                throw new IllegalArgumentException(
                    "star tree index is under an experimental feature and can be activated only by enabling "
                        + FeatureFlags.STAR_TREE_INDEX_SETTING.getKey()
                        + " feature flag in the JVM options"
                );
            }
            if (StarTreeIndexSettings.IS_COMPOSITE_INDEX_SETTING.get(parserContext.getSettings()) == false) {
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "Set '%s' as true as part of index settings to use star tree index",
                        StarTreeIndexSettings.IS_COMPOSITE_INDEX_SETTING.getKey()
                    )
                );
            }
            Iterator<Map.Entry<String, Object>> iterator = compositeNode.entrySet().iterator();
            if (compositeNode.size() > StarTreeIndexSettings.STAR_TREE_MAX_FIELDS_SETTING.get(parserContext.getSettings())) {
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "Composite fields cannot have more than [%s] fields",
                        StarTreeIndexSettings.STAR_TREE_MAX_FIELDS_SETTING.get(parserContext.getSettings())
                    )
                );
            }
            while (iterator.hasNext()) {
                Map.Entry<String, Object> entry = iterator.next();
                String fieldName = entry.getKey();
                // Should accept empty arrays, as a work around for when the
                // user can't provide an empty Map. (PHP for example)
                boolean isEmptyList = entry.getValue() instanceof List && ((List<?>) entry.getValue()).isEmpty();
                if (entry.getValue() instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> propNode = (Map<String, Object>) entry.getValue();
                    String type;
                    Object typeNode = propNode.get("type");
                    if (typeNode != null) {
                        type = typeNode.toString();
                    } else {
                        // lets see if we can derive this...
                        throw new MapperParsingException("No type specified for field [" + fieldName + "]");
                    }
                    Mapper.TypeParser typeParser = getSupportedCompositeTypeParser(type, parserContext);
                    if (typeParser == null) {
                        throw new MapperParsingException("No handler for type [" + type + "] declared on field [" + fieldName + "]");
                    }
                    String[] fieldNameParts = fieldName.split("\\.");
                    // field name is just ".", which is invalid
                    if (fieldNameParts.length < 1) {
                        throw new MapperParsingException("Invalid field name " + fieldName);
                    }
                    String realFieldName = fieldNameParts[fieldNameParts.length - 1];
                    Mapper.Builder<?> fieldBuilder = typeParser.parse(realFieldName, propNode, parserContext, objBuilder);
                    for (int i = fieldNameParts.length - 2; i >= 0; --i) {
                        ObjectMapper.Builder<?> intermediate = new ObjectMapper.Builder<>(fieldNameParts[i]);
                        intermediate.add(fieldBuilder);
                        fieldBuilder = intermediate;
                    }
                    objBuilder.add(fieldBuilder);
                    propNode.remove("type");
                    DocumentMapperParser.checkNoRemainingFields(fieldName, propNode, parserContext.indexVersionCreated());
                    iterator.remove();
                } else if (isEmptyList) {
                    iterator.remove();
                } else {
                    throw new MapperParsingException(
                        "Expected map for property [fields] on field [" + fieldName + "] but got a " + fieldName.getClass()
                    );
                }
            }

            DocumentMapperParser.checkNoRemainingFields(
                compositeNode,
                parserContext.indexVersionCreated(),
                "DocType mapping definition has unsupported parameters: "
            );
        }

        private static Mapper.TypeParser getSupportedCompositeTypeParser(String type, ParserContext parserContext) {
            switch (type) {
                case StarTreeMapper.CONTENT_TYPE:
                    return parserContext.typeParser(type);
                default:
                    throw new IllegalArgumentException(
                        String.format(Locale.ROOT, "Type [%s] isn't supported in composite field context.", type)
                    );
            }
        }

        protected static void parseProperties(ObjectMapper.Builder objBuilder, Map<String, Object> propsNode, ParserContext parserContext) {
            Iterator<Map.Entry<String, Object>> iterator = propsNode.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, Object> entry = iterator.next();
                String fieldName = entry.getKey();
                // Should accept empty arrays, as a work around for when the
                // user can't provide an empty Map. (PHP for example)
                boolean isEmptyList = entry.getValue() instanceof List && ((List<?>) entry.getValue()).isEmpty();

                if (entry.getValue() instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> propNode = (Map<String, Object>) entry.getValue();
                    String type;
                    Object typeNode = propNode.get("type");
                    if (typeNode != null) {
                        type = typeNode.toString();
                    } else {
                        // lets see if we can derive this...
                        if (propNode.get("properties") != null) {
                            type = ObjectMapper.CONTENT_TYPE;
                        } else if (propNode.size() == 1 && propNode.get("enabled") != null) {
                            // if there is a single property with the enabled
                            // flag on it, make it an object
                            // (usually, setting enabled to false to not index
                            // any type, including core values, which
                            type = ObjectMapper.CONTENT_TYPE;
                        } else {
                            throw new MapperParsingException("No type specified for field [" + fieldName + "]");
                        }
                    }

                    Mapper.TypeParser typeParser = parserContext.typeParser(type);
                    if (typeParser == null) {
                        throw new MapperParsingException("No handler for type [" + type + "] declared on field [" + fieldName + "]");
                    }
                    String[] fieldNameParts = fieldName.split("\\.");
                    // field name is just ".", which is invalid
                    if (fieldNameParts.length < 1) {
                        throw new MapperParsingException("Invalid field name " + fieldName);
                    }
                    String realFieldName = fieldNameParts[fieldNameParts.length - 1];
                    Mapper.Builder<?> fieldBuilder = typeParser.parse(realFieldName, propNode, parserContext);
                    for (int i = fieldNameParts.length - 2; i >= 0; --i) {
                        ObjectMapper.Builder<?> intermediate = new ObjectMapper.Builder<>(fieldNameParts[i]);
                        intermediate.add(fieldBuilder);
                        fieldBuilder = intermediate;
                    }
                    objBuilder.add(fieldBuilder);
                    propNode.remove("type");
                    DocumentMapperParser.checkNoRemainingFields(fieldName, propNode, parserContext.indexVersionCreated());
                    iterator.remove();
                } else if (isEmptyList) {
                    iterator.remove();
                } else {
                    throw new MapperParsingException(
                        "Expected map for property [fields] on field [" + fieldName + "] but got a " + fieldName.getClass()
                    );
                }
            }

            DocumentMapperParser.checkNoRemainingFields(
                propsNode,
                parserContext.indexVersionCreated(),
                "DocType mapping definition has unsupported parameters: "
            );

        }

    }

    private final String fullPath;

    private Explicit<Boolean> enabled;

    private final Nested nested;

    private final String nestedTypePath;

    private final Query nestedTypeFilter;

    private volatile Dynamic dynamic;

    private volatile CopyOnWriteHashMap<String, Mapper> mappers;

    ObjectMapper(
        String name,
        String fullPath,
        Explicit<Boolean> enabled,
        Nested nested,
        Dynamic dynamic,
        Map<String, Mapper> mappers,
        Settings settings
    ) {
        super(name);
        assert settings != null;
        if (name.isEmpty()) {
            throw new IllegalArgumentException("name cannot be empty string");
        }
        this.fullPath = fullPath;
        this.enabled = enabled;
        this.nested = nested;
        this.dynamic = dynamic;
        if (mappers == null) {
            this.mappers = new CopyOnWriteHashMap<>();
        } else {
            this.mappers = CopyOnWriteHashMap.copyOf(mappers);
        }
        Version version = IndexMetadata.indexCreated(settings);
        if (version.before(Version.V_2_0_0)) {
            this.nestedTypePath = "__" + fullPath;
        } else {
            this.nestedTypePath = fullPath;
        }
        this.nestedTypeFilter = NestedPathFieldMapper.filter(version, nestedTypePath);
    }

    @Override
    protected ObjectMapper clone() {
        ObjectMapper clone;
        try {
            clone = (ObjectMapper) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
        return clone;
    }

    /**
     * Build a mapping update with the provided sub mapping update.
     */
    public ObjectMapper mappingUpdate(Mapper mapper) {
        ObjectMapper mappingUpdate = clone();
        // reset the sub mappers
        mappingUpdate.mappers = new CopyOnWriteHashMap<>();
        mappingUpdate.putMapper(mapper);
        return mappingUpdate;
    }

    @Override
    public String name() {
        return this.fullPath;
    }

    @Override
    public String typeName() {
        return CONTENT_TYPE;
    }

    public boolean isEnabled() {
        return this.enabled.value();
    }

    public Mapper getMapper(String field) {
        return mappers.get(field);
    }

    public Nested nested() {
        return this.nested;
    }

    public Query nestedTypeFilter() {
        return this.nestedTypeFilter;
    }

    protected void putMapper(Mapper mapper) {
        mappers = mappers.copyAndPut(mapper.simpleName(), mapper);
    }

    @Override
    public Iterator<Mapper> iterator() {
        return mappers.values().iterator();
    }

    public String fullPath() {
        return this.fullPath;
    }

    public String nestedTypePath() {
        return nestedTypePath;
    }

    public final Dynamic dynamic() {
        return dynamic;
    }

    /**
     * Returns the parent {@link ObjectMapper} instance of the specified object mapper or <code>null</code> if there
     * isn't any.
     */
    public ObjectMapper getParentObjectMapper(MapperService mapperService) {
        int indexOfLastDot = fullPath().lastIndexOf('.');
        if (indexOfLastDot != -1) {
            String parentNestObjectPath = fullPath().substring(0, indexOfLastDot);
            return mapperService.getObjectMapper(parentNestObjectPath);
        } else {
            return null;
        }
    }

    /**
     * Returns whether all parent objects fields are nested too.
     */
    public boolean parentObjectMapperAreNested(MapperService mapperService) {
        for (ObjectMapper parent = getParentObjectMapper(mapperService); parent != null; parent = parent.getParentObjectMapper(
            mapperService
        )) {

            if (parent.nested().isNested() == false) {
                return false;
            }
        }
        return true;
    }

    @Override
    public ObjectMapper merge(Mapper mergeWith) {
        return merge(mergeWith, MergeReason.MAPPING_UPDATE);
    }

    @Override
    public void validate(MappingLookup mappers) {
        for (Mapper mapper : this.mappers.values()) {
            mapper.validate(mappers);
        }
    }

    public ObjectMapper merge(Mapper mergeWith, MergeReason reason) {
        if (!(mergeWith instanceof ObjectMapper)) {
            throw new IllegalArgumentException("can't merge a non object mapping [" + mergeWith.name() + "] with an object mapping");
        }
        ObjectMapper mergeWithObject = (ObjectMapper) mergeWith;
        ObjectMapper merged = clone();
        merged.doMerge(mergeWithObject, reason);
        return merged;
    }

    protected void doMerge(final ObjectMapper mergeWith, MergeReason reason) {
        nested().merge(mergeWith.nested(), reason);

        if (mergeWith.dynamic != null) {
            this.dynamic = mergeWith.dynamic;
        }

        if (reason == MergeReason.INDEX_TEMPLATE) {
            if (mergeWith.enabled.explicit()) {
                this.enabled = mergeWith.enabled;
            }
        } else if (isEnabled() != mergeWith.isEnabled()) {
            throw new MapperException("the [enabled] parameter can't be updated for the object mapping [" + name() + "]");
        }

        for (Mapper mergeWithMapper : mergeWith) {
            Mapper mergeIntoMapper = mappers.get(mergeWithMapper.simpleName());

            Mapper merged;
            if (mergeIntoMapper == null) {
                merged = mergeWithMapper;
            } else if (mergeIntoMapper instanceof ObjectMapper) {
                ObjectMapper objectMapper = (ObjectMapper) mergeIntoMapper;
                merged = objectMapper.merge(mergeWithMapper, reason);
            } else {
                assert mergeIntoMapper instanceof FieldMapper || mergeIntoMapper instanceof FieldAliasMapper;
                if (mergeWithMapper instanceof ObjectMapper) {
                    throw new IllegalArgumentException(
                        "can't merge a non object mapping [" + mergeWithMapper.name() + "] with an object mapping"
                    );
                }

                // If we're merging template mappings when creating an index, then a field definition always
                // replaces an existing one.
                if (reason == MergeReason.INDEX_TEMPLATE) {
                    merged = mergeWithMapper;
                } else {
                    merged = mergeIntoMapper.merge(mergeWithMapper);
                }
            }
            putMapper(merged);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        toXContent(builder, params, null);
        return builder;
    }

    public void toXContent(XContentBuilder builder, Params params, ToXContent custom) throws IOException {
        builder.startObject(simpleName());
        if (nested.isNested()) {
            builder.field("type", NESTED_CONTENT_TYPE);
            if (nested.isIncludeInParent()) {
                builder.field("include_in_parent", true);
            }
            if (nested.isIncludeInRoot()) {
                builder.field("include_in_root", true);
            }
        } else if (mappers.isEmpty() && custom == null) {
            // only write the object content type if there are no properties, otherwise, it is automatically detected
            builder.field("type", CONTENT_TYPE);
        }
        if (dynamic != null) {
            builder.field("dynamic", dynamic.name().toLowerCase(Locale.ROOT));
        }
        if (isEnabled() != Defaults.ENABLED) {
            builder.field("enabled", enabled.value());
        }

        if (custom != null) {
            custom.toXContent(builder, params);
        }

        doXContent(builder, params);

        // sort the mappers so we get consistent serialization format
        Mapper[] derivedSortedMappers = mappers.values()
            .stream()
            .filter(m -> m instanceof DerivedFieldMapper)
            .toArray(size -> new Mapper[size]);
        Arrays.sort(derivedSortedMappers, new Comparator<Mapper>() {
            @Override
            public int compare(Mapper o1, Mapper o2) {
                return o1.name().compareTo(o2.name());
            }
        });

        Mapper[] sortedMappers = mappers.values()
            .stream()
            .filter(m -> !(m instanceof DerivedFieldMapper))
            .toArray(size -> new Mapper[size]);
        Arrays.sort(sortedMappers, new Comparator<Mapper>() {
            @Override
            public int compare(Mapper o1, Mapper o2) {
                return o1.name().compareTo(o2.name());
            }
        });

        int count = 0;
        for (Mapper mapper : derivedSortedMappers) {
            if (count++ == 0) {
                builder.startObject("derived");
            }
            mapper.toXContent(builder, params);
        }
        if (count > 0) {
            builder.endObject();
        }

        count = 0;
        for (Mapper mapper : sortedMappers) {
            if (!(mapper instanceof MetadataFieldMapper)) {
                if (count++ == 0) {
                    builder.startObject("properties");
                }
                mapper.toXContent(builder, params);
            }
        }
        if (count > 0) {
            builder.endObject();
        }
        builder.endObject();
    }

    protected void doXContent(XContentBuilder builder, Params params) throws IOException {

    }

}
