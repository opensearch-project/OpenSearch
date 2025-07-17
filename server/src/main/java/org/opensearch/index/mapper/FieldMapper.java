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

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafReader;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.AbstractXContentParser;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.analysis.NamedAnalyzer;
import org.opensearch.index.mapper.FieldNamesFieldMapper.FieldNamesFieldType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Spliterators;
import java.util.TreeMap;
import java.util.stream.StreamSupport;

/**
 * The base OpenSearch Field Mapper
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public abstract class FieldMapper extends Mapper implements Cloneable {
    public static final Setting<Boolean> IGNORE_MALFORMED_SETTING = Setting.boolSetting(
        "index.mapping.ignore_malformed",
        false,
        Property.IndexScope
    );
    public static final Setting<Boolean> COERCE_SETTING = Setting.boolSetting("index.mapping.coerce", false, Property.IndexScope);

    /**
     * Base builder for all field mappers
     *
     * @opensearch.internal
     */
    public abstract static class Builder<T extends Builder<T>> extends Mapper.Builder<T> {

        protected final FieldType fieldType;
        protected boolean omitNormsSet = false;
        protected boolean indexOptionsSet = false;
        protected boolean hasDocValues = true;
        protected boolean indexed = true;
        protected final MultiFields.Builder multiFieldsBuilder;
        protected CopyTo copyTo = CopyTo.empty();
        protected float boost = 1.0f;
        protected Map<String, String> meta = Collections.emptyMap();
        // TODO move to KeywordFieldMapper.Builder
        protected boolean eagerGlobalOrdinals;
        // TODO move to text-specific builder base class
        protected NamedAnalyzer indexAnalyzer;
        protected NamedAnalyzer searchAnalyzer;
        protected NamedAnalyzer searchQuoteAnalyzer;

        protected Builder(String name, FieldType fieldType) {
            super(name);
            this.fieldType = new FieldType(fieldType);
            multiFieldsBuilder = new MultiFields.Builder();
        }

        public T index(boolean index) {
            this.indexed = index;
            if (index == false) {
                this.fieldType.setIndexOptions(IndexOptions.NONE);
            }
            return builder;
        }

        public T store(boolean store) {
            this.fieldType.setStored(store);
            return builder;
        }

        public T docValues(boolean docValues) {
            this.hasDocValues = docValues;
            return builder;
        }

        public T storeTermVectors(boolean termVectors) {
            if (termVectors != this.fieldType.storeTermVectors()) {
                this.fieldType.setStoreTermVectors(termVectors);
            } // don't set it to false, it is default and might be flipped by a more specific option
            return builder;
        }

        public T storeTermVectorOffsets(boolean termVectorOffsets) {
            if (termVectorOffsets) {
                this.fieldType.setStoreTermVectors(termVectorOffsets);
            }
            this.fieldType.setStoreTermVectorOffsets(termVectorOffsets);
            return builder;
        }

        public T storeTermVectorPositions(boolean termVectorPositions) {
            if (termVectorPositions) {
                this.fieldType.setStoreTermVectors(termVectorPositions);
            }
            this.fieldType.setStoreTermVectorPositions(termVectorPositions);
            return builder;
        }

        public T storeTermVectorPayloads(boolean termVectorPayloads) {
            if (termVectorPayloads) {
                this.fieldType.setStoreTermVectors(termVectorPayloads);
            }
            this.fieldType.setStoreTermVectorPayloads(termVectorPayloads);
            return builder;
        }

        public T boost(float boost) {
            this.boost = boost;
            return builder;
        }

        public T omitNorms(boolean omitNorms) {
            this.fieldType.setOmitNorms(omitNorms);
            this.omitNormsSet = true;
            return builder;
        }

        public T indexOptions(IndexOptions indexOptions) {
            this.fieldType.setIndexOptions(indexOptions);
            this.indexOptionsSet = true;
            return builder;
        }

        public T indexAnalyzer(NamedAnalyzer indexAnalyzer) {
            this.indexAnalyzer = indexAnalyzer;
            return builder;
        }

        public T searchAnalyzer(NamedAnalyzer searchAnalyzer) {
            this.searchAnalyzer = searchAnalyzer;
            return builder;
        }

        public T searchQuoteAnalyzer(NamedAnalyzer searchQuoteAnalyzer) {
            this.searchQuoteAnalyzer = searchQuoteAnalyzer;
            return builder;
        }

        public T setEagerGlobalOrdinals(boolean eagerGlobalOrdinals) {
            this.eagerGlobalOrdinals = eagerGlobalOrdinals;
            return builder;
        }

        public T addMultiField(Mapper.Builder<?> mapperBuilder) {
            multiFieldsBuilder.add(mapperBuilder);
            return builder;
        }

        public T copyTo(CopyTo copyTo) {
            this.copyTo = copyTo;
            return builder;
        }

        protected String buildFullName(BuilderContext context) {
            return context.path().pathAsText(name);
        }

        /** Set metadata on this field. */
        public T meta(Map<String, String> meta) {
            this.meta = meta;
            return (T) this;
        }
    }

    protected FieldType fieldType;
    protected MappedFieldType mappedFieldType;
    protected MultiFields multiFields;
    protected CopyTo copyTo;
    protected DerivedFieldGenerator derivedFieldGenerator;

    protected FieldMapper(String simpleName, FieldType fieldType, MappedFieldType mappedFieldType, MultiFields multiFields, CopyTo copyTo) {
        super(simpleName);
        if (mappedFieldType.name().isEmpty()) {
            throw new IllegalArgumentException("name cannot be empty string");
        }
        fieldType.freeze();
        this.fieldType = fieldType;
        this.mappedFieldType = mappedFieldType;
        this.multiFields = multiFields;
        this.copyTo = Objects.requireNonNull(copyTo);
        this.derivedFieldGenerator = derivedFieldGenerator();
    }

    @Override
    public String name() {
        return fieldType().name();
    }

    @Override
    public String typeName() {
        return mappedFieldType.typeName();
    }

    public MappedFieldType fieldType() {
        return mappedFieldType;
    }

    /**
     * List of fields where this field should be copied to
     */
    public CopyTo copyTo() {
        return copyTo;
    }

    public MultiFields multiFields() {
        return multiFields;
    }

    /**
     * Whether this mapper can handle an array value during document parsing. If true,
     * when an array is encountered during parsing, the document parser will pass the
     * whole array to the mapper. If false, the array is split into individual values
     * and each value is passed to the mapper for parsing.
     */
    public boolean parsesArrayValue() {
        return false;
    }

    /**
     * Parse the field value using the provided {@link ParseContext}.
     */
    public void parse(ParseContext context) throws IOException {
        try {
            parseCreateField(context);
        } catch (Exception e) {
            boolean ignore_malformed = false;
            if (context.indexSettings() != null) ignore_malformed = IGNORE_MALFORMED_SETTING.get(context.indexSettings().getSettings());
            String valuePreview = "";
            try {
                XContentParser parser = context.parser();
                Object complexValue = AbstractXContentParser.readValue(parser, HashMap::new);
                if (complexValue == null) {
                    valuePreview = "null";
                } else {
                    valuePreview = complexValue.toString();
                }
            } catch (Exception innerException) {
                if (ignore_malformed == false) {
                    throw new MapperParsingException(
                        "failed to parse field [{}] of type [{}] in document with id '{}'. " + "Could not parse field value preview,",
                        e,
                        fieldType().name(),
                        fieldType().typeName(),
                        context.sourceToParse().id()
                    );
                }
            }

            if (ignore_malformed == false) {
                throw new MapperParsingException(
                    "failed to parse field [{}] of type [{}] in document with id '{}'. " + "Preview of field's value: '{}'",
                    e,
                    fieldType().name(),
                    fieldType().typeName(),
                    context.sourceToParse().id(),
                    valuePreview
                );
            }
        }
        multiFields.parse(this, context);
    }

    /**
     * Parse the field value and populate the fields on {@link ParseContext#doc()}.
     * <p>
     * Implementations of this method should ensure that on failing to parse parser.currentToken() must be the
     * current failing token
     */
    protected abstract void parseCreateField(ParseContext context) throws IOException;

    protected final void createFieldNamesField(ParseContext context) {
        assert fieldType().hasDocValues() == false : "_field_names should only be used when doc_values are turned off";
        FieldNamesFieldType fieldNamesFieldType = context.docMapper().metadataMapper(FieldNamesFieldMapper.class).fieldType();
        if (fieldNamesFieldType != null && fieldNamesFieldType.isEnabled()) {
            for (String fieldName : FieldNamesFieldMapper.extractFieldNames(fieldType().name())) {
                context.doc().add(new Field(FieldNamesFieldMapper.NAME, fieldName, FieldNamesFieldMapper.Defaults.FIELD_TYPE));
            }
        }
    }

    @Override
    public Iterator<Mapper> iterator() {
        return multiFields.iterator();
    }

    @Override
    protected FieldMapper clone() {
        try {
            return (FieldMapper) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new AssertionError(e);
        }
    }

    @Override
    public final void validate(MappingLookup mappers) {
        if (this.copyTo() != null && this.copyTo().copyToFields().isEmpty() == false) {
            if (mappers.isMultiField(this.name())) {
                throw new IllegalArgumentException("[copy_to] may not be used to copy from a multi-field: [" + this.name() + "]");
            }

            final String sourceScope = mappers.getNestedScope(this.name());
            for (String copyTo : this.copyTo().copyToFields()) {
                if (mappers.isMultiField(copyTo)) {
                    throw new IllegalArgumentException("[copy_to] may not be used to copy to a multi-field: [" + copyTo + "]");
                }
                if (mappers.isObjectField(copyTo)) {
                    throw new IllegalArgumentException("Cannot copy to field [" + copyTo + "] since it is mapped as an object");
                }

                final String targetScope = mappers.getNestedScope(copyTo);
                checkNestedScopeCompatibility(sourceScope, targetScope);
            }
        }
        for (Mapper multiField : multiFields()) {
            multiField.validate(mappers);
        }
        doValidate(mappers);
    }

    protected void doValidate(MappingLookup mappers) {}

    private static void checkNestedScopeCompatibility(String source, String target) {
        boolean targetIsParentOfSource;
        if (source == null || target == null) {
            targetIsParentOfSource = target == null;
        } else {
            targetIsParentOfSource = source.equals(target) || source.startsWith(target + ".");
        }
        if (targetIsParentOfSource == false) {
            throw new IllegalArgumentException(
                "Illegal combination of [copy_to] and [nested] mappings: [copy_to] may only copy data to the current nested "
                    + "document or any of its parents, however one [copy_to] directive is trying to copy data from nested object ["
                    + source
                    + "] to ["
                    + target
                    + "]"
            );
        }
    }

    @Override
    public FieldMapper merge(Mapper mergeWith) {
        FieldMapper merged = clone();
        List<String> conflicts = new ArrayList<>();
        if (mergeWith instanceof FieldMapper == false) {
            throw new IllegalArgumentException(
                "mapper ["
                    + mappedFieldType.name()
                    + "] cannot be changed from type ["
                    + contentType()
                    + "] to ["
                    + mergeWith.getClass().getSimpleName()
                    + "]"
            );
        }
        FieldMapper toMerge = (FieldMapper) mergeWith;
        merged.mergeSharedOptions(toMerge, conflicts);
        merged.mergeOptions(toMerge, conflicts);
        if (conflicts.isEmpty() == false) {
            throw new IllegalArgumentException("Mapper for [" + name() + "] conflicts with existing mapping:\n" + conflicts.toString());
        }
        merged.multiFields = multiFields.merge(toMerge.multiFields);
        // apply changeable values
        merged.mappedFieldType = toMerge.mappedFieldType;
        merged.fieldType = toMerge.fieldType;
        merged.copyTo = toMerge.copyTo;
        return merged;
    }

    private void mergeSharedOptions(FieldMapper mergeWith, List<String> conflicts) {

        if (Objects.equals(this.contentType(), mergeWith.contentType()) == false) {
            throw new IllegalArgumentException(
                "mapper ["
                    + fieldType().name()
                    + "] cannot be changed from type ["
                    + contentType()
                    + "] to ["
                    + mergeWith.contentType()
                    + "]"
            );
        }

        FieldType other = mergeWith.fieldType;
        MappedFieldType otherm = mergeWith.mappedFieldType;

        boolean indexed = fieldType.indexOptions() != IndexOptions.NONE;
        boolean mergeWithIndexed = other.indexOptions() != IndexOptions.NONE;
        if (indexed != mergeWithIndexed) {
            conflicts.add("mapper [" + name() + "] has different [index] values");
        }
        // TODO: should be validating if index options go "up" (but "down" is ok)
        if (fieldType.indexOptions() != other.indexOptions()) {
            conflicts.add("mapper [" + name() + "] has different [index_options] values");
        }
        if (fieldType.stored() != other.stored()) {
            conflicts.add("mapper [" + name() + "] has different [store] values");
        }
        if (this.mappedFieldType.hasDocValues() != otherm.hasDocValues()) {
            conflicts.add("mapper [" + name() + "] has different [doc_values] values");
        }
        if (fieldType.omitNorms() && !other.omitNorms()) {
            conflicts.add("mapper [" + name() + "] has different [norms] values, cannot change from disable to enabled");
        }
        if (fieldType.storeTermVectors() != other.storeTermVectors()) {
            conflicts.add("mapper [" + name() + "] has different [term_vector] values");
        }
        if (fieldType.storeTermVectorOffsets() != other.storeTermVectorOffsets()) {
            conflicts.add("mapper [" + name() + "] has different [store_term_vector_offsets] values");
        }
        if (fieldType.storeTermVectorPositions() != other.storeTermVectorPositions()) {
            conflicts.add("mapper [" + name() + "] has different [store_term_vector_positions] values");
        }
        if (fieldType.storeTermVectorPayloads() != other.storeTermVectorPayloads()) {
            conflicts.add("mapper [" + name() + "] has different [store_term_vector_payloads] values");
        }

        // null and "default"-named index analyzers both mean the default is used
        if (mappedFieldType.indexAnalyzer() == null || "default".equals(mappedFieldType.indexAnalyzer().name())) {
            if (otherm.indexAnalyzer() != null && "default".equals(otherm.indexAnalyzer().name()) == false) {
                conflicts.add("mapper [" + name() + "] has different [analyzer]");
            }
        } else if (otherm.indexAnalyzer() == null || "default".equals(otherm.indexAnalyzer().name())) {
            conflicts.add("mapper [" + name() + "] has different [analyzer]");
        } else if (mappedFieldType.indexAnalyzer().name().equals(otherm.indexAnalyzer().name()) == false) {
            conflicts.add("mapper [" + name() + "] has different [analyzer]");
        }
    }

    /**
     * Merge type-specific options and check for incompatible settings in mappings to be merged
     */
    protected abstract void mergeOptions(FieldMapper other, List<String> conflicts);

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(simpleName());
        boolean includeDefaults = params.paramAsBoolean("include_defaults", false);
        doXContentBody(builder, includeDefaults, params);
        return builder.endObject();
    }

    protected boolean indexedByDefault() {
        return true;
    }

    protected boolean docValuesByDefault() {
        return true;
    }

    protected boolean storedByDefault() {
        return false;
    }

    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {

        builder.field("type", contentType());

        if (includeDefaults || fieldType().boost() != 1.0f) {
            builder.field("boost", fieldType().boost());
        }

        if (includeDefaults || mappedFieldType.isSearchable() != indexedByDefault()) {
            builder.field("index", mappedFieldType.isSearchable());
        }
        if (includeDefaults || mappedFieldType.hasDocValues() != docValuesByDefault()) {
            builder.field("doc_values", mappedFieldType.hasDocValues());
        }
        if (includeDefaults || fieldType.stored() != storedByDefault()) {
            builder.field("store", fieldType.stored());
        }

        multiFields.toXContent(builder, params);
        copyTo.toXContent(builder, params);

        if (includeDefaults || fieldType().meta().isEmpty() == false) {
            builder.field("meta", new TreeMap<>(fieldType().meta())); // ensure consistent order
        }
    }

    protected final void doXContentAnalyzers(XContentBuilder builder, boolean includeDefaults) throws IOException {
        if (fieldType.tokenized() == false) {
            return;
        }
        if (fieldType().indexAnalyzer() == null) {
            if (includeDefaults) {
                builder.field("analyzer", "default");
            }
        } else {
            boolean hasDefaultIndexAnalyzer = fieldType().indexAnalyzer().name().equals("default");
            final String searchAnalyzerName = fieldType().getTextSearchInfo().getSearchAnalyzer() == null
                ? "default"
                : fieldType().getTextSearchInfo().getSearchAnalyzer().name();
            final String searchQuoteAnalyzerName = fieldType().getTextSearchInfo().getSearchQuoteAnalyzer() == null
                ? searchAnalyzerName
                : fieldType().getTextSearchInfo().getSearchQuoteAnalyzer().name();
            boolean hasDifferentSearchAnalyzer = searchAnalyzerName.equals(fieldType().indexAnalyzer().name()) == false;
            boolean hasDifferentSearchQuoteAnalyzer = Objects.equals(searchAnalyzerName, searchQuoteAnalyzerName) == false;
            if (includeDefaults || hasDefaultIndexAnalyzer == false || hasDifferentSearchAnalyzer || hasDifferentSearchQuoteAnalyzer) {
                builder.field("analyzer", fieldType().indexAnalyzer().name());
                if (includeDefaults || hasDifferentSearchAnalyzer || hasDifferentSearchQuoteAnalyzer) {
                    builder.field("search_analyzer", searchAnalyzerName);
                    if (includeDefaults || hasDifferentSearchQuoteAnalyzer) {
                        builder.field("search_quote_analyzer", searchQuoteAnalyzerName);
                    }
                }
            }
        }
    }

    protected static String indexOptionToString(IndexOptions indexOption) {
        switch (indexOption) {
            case DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS:
                return TypeParsers.INDEX_OPTIONS_OFFSETS;
            case DOCS_AND_FREQS:
                return TypeParsers.INDEX_OPTIONS_FREQS;
            case DOCS_AND_FREQS_AND_POSITIONS:
                return TypeParsers.INDEX_OPTIONS_POSITIONS;
            case DOCS:
                return TypeParsers.INDEX_OPTIONS_DOCS;
            default:
                throw new IllegalArgumentException("Unknown IndexOptions [" + indexOption + "]");
        }
    }

    protected abstract String contentType();

    /**
     * Method to create derived source generator for this field mapper, it is illegal to enable the
     * derived source feature and not implement this method for a field mapper
     */
    protected DerivedFieldGenerator derivedFieldGenerator() {
        return null;
    }

    protected void setDerivedFieldGenerator(DerivedFieldGenerator derivedFieldGenerator) {
        this.derivedFieldGenerator = derivedFieldGenerator;
    }

    protected DerivedFieldGenerator getDerivedFieldGenerator() {
        return this.derivedFieldGenerator;
    }

    /**
     * Method to determine, if it is possible to derive source for this field using field mapping parameters.
     * DerivedFieldGenerator should be set for which derived source feature is supported, this behaviour can be
     * overridden at a Mapper level by implementing this method
     */
    public void canDeriveSource() {
        if (this.copyTo() != null && !this.copyTo().copyToFields().isEmpty()) {
            throw new UnsupportedOperationException("Unable to derive source for fields with copy_to parameter set");
        }
        canDeriveSourceInternal();
        if (getDerivedFieldGenerator() == null) {
            throw new UnsupportedOperationException(
                "Derive source is not supported for field [" + name() + "] with field " + "type [" + fieldType().typeName() + "]"
            );
        }
    }

    /**
     * Must be overridden for each mapper for which derived source feature is supported
     */
    protected void canDeriveSourceInternal() {
        throw new UnsupportedOperationException(
            "Derive source is not supported for field [" + name() + "] with field " + "type [" + fieldType().typeName() + "]"
        );
    }

    /**
     * Validates if doc values is enabled for a field or not
     */
    void checkDocValuesForDerivedSource() {
        if (!mappedFieldType.hasDocValues()) {
            throw new UnsupportedOperationException("Unable to derive source for [" + name() + "] with doc values disabled");
        }
    }

    /**
     * Validates if stored field is enabled for a field or not
     */
    void checkStoredForDerivedSource() {
        if (!mappedFieldType.isStored()) {
            throw new UnsupportedOperationException("Unable to derive source for [" + name() + "] with store disabled");
        }
    }

    /**
     * Validates if doc_values or stored field is enabled for a field or not
     */
    void checkStoredAndDocValuesForDerivedSource() {
        if (!mappedFieldType.isStored() && !mappedFieldType.hasDocValues()) {
            throw new UnsupportedOperationException("Unable to derive source for [" + name() + "] with stored and " + "docValues disabled");
        }
    }

    /**
     * Method used for deriving source and building it to XContentBuilder object
     * <p>
     * Considerations:
     *  1. If "null_value" is defined in field mapping and if ingested doc contains "null" field value then derived
     *     source will contain "null_value" as a displayed field value instead of null and if "null_value" is not
     *     defined in mapping then field itself will not be present in derived source
     *
     * @param builder - builder to store the derived source filed
     * @param leafReader - leafReader to read data from
     * @param docId - docId for which we want to derive the source
     */
    public void deriveSource(XContentBuilder builder, LeafReader leafReader, int docId) throws IOException {
        derivedFieldGenerator.generate(builder, leafReader, docId);
    }

    /**
     * Multi field implementation used across field mappers
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class MultiFields implements Iterable<Mapper> {

        public static MultiFields empty() {
            return new MultiFields(Map.of());
        }

        /**
         * Concrete builder for field mappers
         *
         * @opensearch.internal
         */
        public static class Builder {

            private final Map<String, Mapper.Builder> mapperBuilders = new HashMap<>();

            public Builder add(Mapper.Builder builder) {
                mapperBuilders.put(builder.name(), builder);
                return this;
            }

            public Builder add(Mapper mapper) {
                mapperBuilders.put(mapper.simpleName(), new Mapper.Builder(mapper.simpleName()) {
                    @Override
                    public Mapper build(BuilderContext context) {
                        return mapper;
                    }
                });
                return this;
            }

            public Builder update(Mapper toMerge, ContentPath contentPath) {
                if (mapperBuilders.containsKey(toMerge.simpleName()) == false) {
                    add(toMerge);
                } else {
                    Mapper.Builder builder = mapperBuilders.get(toMerge.simpleName());
                    Mapper existing = builder.build(new BuilderContext(Settings.EMPTY, contentPath));
                    add(existing.merge(toMerge));
                }
                return this;
            }

            @SuppressWarnings("unchecked")
            public MultiFields build(Mapper.Builder mainFieldBuilder, BuilderContext context) {
                if (mapperBuilders.isEmpty()) {
                    return empty();
                } else {
                    context.path().add(mainFieldBuilder.name());
                    Map mapperBuilders = this.mapperBuilders;
                    for (final Map.Entry<String, Mapper.Builder> cursor : this.mapperBuilders.entrySet()) {
                        String key = cursor.getKey();
                        Mapper.Builder value = cursor.getValue();
                        Mapper mapper = value.build(context);
                        assert mapper instanceof FieldMapper;
                        mapperBuilders.put(key, mapper);
                    }
                    context.path().remove();
                    final Map<String, FieldMapper> mappers = (Map<String, FieldMapper>) mapperBuilders;
                    return new MultiFields(mappers);
                }
            }
        }

        private final Map<String, FieldMapper> mappers;

        private MultiFields(final Map<String, FieldMapper> mappers) {
            this.mappers = Collections.unmodifiableMap(mappers);
        }

        public void parse(FieldMapper mainField, ParseContext context) throws IOException {
            // TODO: multi fields are really just copy fields, we just need to expose "sub fields" or something that can be part
            // of the mappings
            if (mappers.isEmpty()) {
                return;
            }

            context = context.createMultiFieldContext();
            context.path().add(mainField.simpleName());
            for (final FieldMapper cursor : mappers.values()) {
                cursor.parse(context);
            }
            context.path().remove();
        }

        public MultiFields merge(MultiFields mergeWith) {
            final Map<String, FieldMapper> newMappersBuilder = new HashMap<>(mappers);

            for (final FieldMapper mergeWithMapper : mergeWith.mappers.values()) {
                FieldMapper mergeIntoMapper = mappers.get(mergeWithMapper.simpleName());
                if (mergeIntoMapper == null) {
                    newMappersBuilder.put(mergeWithMapper.simpleName(), mergeWithMapper);
                } else {
                    FieldMapper merged = mergeIntoMapper.merge(mergeWithMapper);
                    newMappersBuilder.put(merged.simpleName(), merged); // override previous definition
                }
            }

            final Map<String, FieldMapper> mappers = Collections.unmodifiableMap(newMappersBuilder);
            return new MultiFields(mappers);
        }

        @Override
        public Iterator<Mapper> iterator() {
            return StreamSupport.stream(Spliterators.spliterator(mappers.values(), 0), false).map((p) -> (Mapper) p).iterator();
        }

        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (!mappers.isEmpty()) {
                // sort the mappers so we get consistent serialization format
                Mapper[] sortedMappers = mappers.values().toArray(new FieldMapper[0]);
                Arrays.sort(sortedMappers, new Comparator<Mapper>() {
                    @Override
                    public int compare(Mapper o1, Mapper o2) {
                        return o1.name().compareTo(o2.name());
                    }
                });
                builder.startObject("fields");
                for (Mapper mapper : sortedMappers) {
                    mapper.toXContent(builder, params);
                }
                builder.endObject();
            }
            return builder;
        }
    }

    /**
     * Represents a list of fields with optional boost factor where the current field should be copied to
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class CopyTo {

        private static final CopyTo EMPTY = new CopyTo(Collections.emptyList());

        public static CopyTo empty() {
            return EMPTY;
        }

        private final List<String> copyToFields;

        private CopyTo(List<String> copyToFields) {
            this.copyToFields = copyToFields;
        }

        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (!copyToFields.isEmpty()) {
                builder.startArray("copy_to");
                for (String field : copyToFields) {
                    builder.value(field);
                }
                builder.endArray();
            }
            return builder;
        }

        /**
         * Builder for the copyTo field
         *
         * @opensearch.internal
         */
        public static class Builder {
            private final List<String> copyToBuilders = new ArrayList<>();

            public Builder add(String field) {
                copyToBuilders.add(field);
                return this;
            }

            public CopyTo build() {
                if (copyToBuilders.isEmpty()) {
                    return EMPTY;
                }
                return new CopyTo(Collections.unmodifiableList(copyToBuilders));
            }

            public void reset(CopyTo copyTo) {
                copyToBuilders.clear();
                copyToBuilders.addAll(copyTo.copyToFields);
            }
        }

        public List<String> copyToFields() {
            return copyToFields;
        }
    }

}
