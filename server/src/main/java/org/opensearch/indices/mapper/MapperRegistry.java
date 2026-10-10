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

package org.opensearch.indices.mapper;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.index.mapper.DynamicFieldTypeInferencer;
import org.opensearch.index.mapper.DynamicTemplateTypeHandler;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.index.mapper.MetadataFieldMapper;
import org.opensearch.plugins.MapperPlugin;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A registry for all field mappers.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class MapperRegistry {

    private final Map<String, Mapper.TypeParser> mapperParsers;
    private final Map<String, MetadataFieldMapper.TypeParser> metadataMapperParsers;
    private final Function<String, Predicate<String>> fieldFilter;
    private final Map<DynamicFieldTypeInferencer, Set<String>> dynamicFieldTypeInferencers;
    private final Map<String, DynamicTemplateTypeHandler> dynamicTemplateTypes;

    public MapperRegistry(
        Map<String, Mapper.TypeParser> mapperParsers,
        Map<String, MetadataFieldMapper.TypeParser> metadataMapperParsers,
        Function<String, Predicate<String>> fieldFilter
    ) {
        this(mapperParsers, metadataMapperParsers, fieldFilter, Collections.emptyMap(), Collections.emptyMap());
    }

    public MapperRegistry(
        Map<String, Mapper.TypeParser> mapperParsers,
        Map<String, MetadataFieldMapper.TypeParser> metadataMapperParsers,
        Function<String, Predicate<String>> fieldFilter,
        Map<DynamicFieldTypeInferencer, Set<String>> dynamicFieldTypeInferencers,
        Map<String, DynamicTemplateTypeHandler> dynamicTemplateTypes
    ) {
        this.mapperParsers = Collections.unmodifiableMap(new LinkedHashMap<>(mapperParsers));
        this.metadataMapperParsers = Collections.unmodifiableMap(new LinkedHashMap<>(metadataMapperParsers));
        this.fieldFilter = fieldFilter;
        this.dynamicFieldTypeInferencers = Collections.unmodifiableMap(new LinkedHashMap<>(dynamicFieldTypeInferencers));
        this.dynamicTemplateTypes = Collections.unmodifiableMap(new LinkedHashMap<>(dynamicTemplateTypes));
    }

    /**
     * Return a map of the mappers that have been registered. The
     * returned map uses the type of the field as a key.
     */
    public Map<String, Mapper.TypeParser> getMapperParsers() {
        return mapperParsers;
    }

    /**
     * Return a map of the meta mappers that have been registered. The
     * returned map uses the name of the field as a key.
     */
    public Map<String, MetadataFieldMapper.TypeParser> getMetadataMapperParsers() {
        return metadataMapperParsers;
    }

    /**
     * Returns true if the provided field is a registered metadata field, false otherwise
     */
    public boolean isMetadataField(String field) {
        return getMetadataMapperParsers().containsKey(field);
    }

    /**
     * Returns a function that, given a concrete index name, returns a predicate that determines field visibility. Consumers include
     * metadata APIs such as get mappings, get index, get field mappings, and field capabilities, as well as APIs that access field values
     * or otherwise operate on payload data. The predicate receives the field name as its input.
     *
     * <p>In case multiple plugins register a field filter through {@link MapperPlugin#getFieldFilter()}, only fields that match all
     * registered filters are returned by get mappings, get index, get field mappings, and field capabilities APIs. Other consumers,
     * including payload-data APIs, must likewise make only fields matching all registered filters available. The same aggregated filter is
     * exposed to query-schema and query-planning implementations.
     *
     * <p>Plugins may use these filters to implement authorization access controls such as field-level security. Consumers must therefore
     * treat the result as an access-control boundary and must not bypass it or reintroduce rejected fields.
     */
    public Function<String, Predicate<String>> getFieldFilter() {
        return fieldFilter;
    }

    /**
     * Returns the registered dynamic field type inferencers, in priority order, each bound to the set
     * of type strings the plugin that supplied it registered as dynamic template types. An inferencer
     * may only produce a type in its bound set.
     */
    public Map<DynamicFieldTypeInferencer, Set<String>> getDynamicFieldTypeInferencers() {
        return dynamicFieldTypeInferencers;
    }

    /** Returns the registered dynamic template type handlers keyed by their match_mapping_type string. */
    public Map<String, DynamicTemplateTypeHandler> getDynamicTemplateTypes() {
        return dynamicTemplateTypes;
    }
}
