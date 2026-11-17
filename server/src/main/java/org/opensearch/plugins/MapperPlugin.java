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

package org.opensearch.plugins;

import org.opensearch.index.mapper.DynamicFieldTypeInferencer;
import org.opensearch.index.mapper.DynamicTemplateTypeHandler;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.index.mapper.MappingTransformer;
import org.opensearch.index.mapper.MetadataFieldMapper;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * An extension point for {@link Plugin} implementations to add custom mappers
 *
 * @opensearch.api
 */
public interface MapperPlugin {

    /**
     * Returns additional mapper implementations added by this plugin.
     * <p>
     * The key of the returned {@link Map} is the unique name for the mapper which will be used
     * as the mapping {@code type}, and the value is a {@link Mapper.TypeParser} to parse the
     * mapper settings into a {@link Mapper}.
     */
    default Map<String, Mapper.TypeParser> getMappers() {
        return Collections.emptyMap();
    }

    /**
     * Returns additional metadata mapper implementations added by this plugin.
     * <p>
     * The key of the returned {@link Map} is the unique name for the metadata mapper, which
     * is used in the mapping json to configure the metadata mapper, and the value is a
     * {@link MetadataFieldMapper.TypeParser} to parse the mapper settings into a
     * {@link MetadataFieldMapper}.
     */
    default Map<String, MetadataFieldMapper.TypeParser> getMetadataMappers() {
        return Collections.emptyMap();
    }

    /**
     * Returns a function that, given a concrete index name, returns a predicate that determines field visibility. Consumers include
     * metadata APIs such as get mappings, get index, get field mappings, and field capabilities, as well as APIs that access field values
     * or otherwise operate on payload data. The predicate receives the field name as its input and should return {@code true} to show the
     * field and {@code false} to hide it.
     *
     * <p>The filter also applies to query schemas and query planning. Query engines must apply it before field-name resolution, wildcard
     * expansion, validation, and logical or physical plan construction so a hidden field cannot be projected, filtered, aggregated,
     * sorted, joined, or passed to a function. The function is evaluated with concrete index names, including when the request used an
     * alias, wildcard, data stream, or other index expression.
     *
     * <p>Implementations may use this filter to enforce authorization access controls such as field-level security. Consumers must
     * therefore treat it as an access-control boundary rather than a presentation-only filter: they must not bypass the predicate or
     * reintroduce rejected fields through another metadata or planning path.
     */
    default Function<String, Predicate<String>> getFieldFilter() {
        return NOOP_FIELD_FILTER;
    }

    /**
     * The default field predicate applied, which doesn't filter anything. That means that by default get mappings, get index
     * get field mappings and field capabilities API will return every field that's present in the mappings.
     */
    Predicate<String> NOOP_FIELD_PREDICATE = field -> true;

    /**
     * The default field filter applied, which doesn't filter anything. That means that by default get mappings, get index
     * get field mappings and field capabilities API will return every field that's present in the mappings.
     */
    Function<String, Predicate<String>> NOOP_FIELD_FILTER = index -> NOOP_FIELD_PREDICATE;

    /**
     * Returns mapper transformer implementations added by this plugin.
     *
     */
    default List<MappingTransformer> getMappingTransformers() {
        return Collections.emptyList();
    }

    /**
     * Returns dynamic field type inferencers provided by this plugin.
     * These are consulted when an unmapped array field is encountered during document indexing
     * and no template matches. The first inferencer to claim a field wins.
     */
    default List<DynamicFieldTypeInferencer> getDynamicFieldTypeInferencers() {
        return Collections.emptyList();
    }

    /**
     * Returns dynamic template types registered by this plugin.
     * These allow plugins to register custom match_mapping_type strings
     * (e.g. "knn_vector") that users can reference in dynamic templates.
     * The key is the type string, and the value is the handler that adjusts
     * the mapper builder when a template matches.
     */
    default Map<String, DynamicTemplateTypeHandler> getDynamicTemplateTypes() {
        return Collections.emptyMap();
    }
}
