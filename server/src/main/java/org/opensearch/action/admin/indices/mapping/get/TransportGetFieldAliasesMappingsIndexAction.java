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

package org.opensearch.action.admin.indices.mapping.get;

import org.opensearch.OpenSearchException;
import org.opensearch.action.admin.indices.mapping.get.GetFieldAliasesMappingsResponse.FieldAliasesMappingMetadata;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.single.shard.TransportSingleShardAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.routing.ShardsIterator;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.common.regex.Regex;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.XContentHelper;
import org.opensearch.index.IndexService;
import org.opensearch.index.mapper.MappingLookup;
import org.opensearch.index.mapper.DocumentMapper;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.index.mapper.FieldTypeLookup;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.indices.IndicesService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static java.util.Collections.singletonMap;

/**
 * Transport action used to retrieve the mappings related to fields that belong to a specific index
 *
 * @opensearch.internal
 */
public class TransportGetFieldAliasesMappingsIndexAction extends TransportSingleShardAction<
    GetFieldAliasesMappingsIndexRequest,
    GetFieldAliasesMappingsResponse> {

    private static final String ACTION_NAME = GetFieldAliasesMappingsAction.NAME + "[index]";

    protected final ClusterService clusterService;
    private final IndicesService indicesService;

    @Inject
    public TransportGetFieldAliasesMappingsIndexAction(
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            ACTION_NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            GetFieldAliasesMappingsIndexRequest::new,
            ThreadPool.Names.MANAGEMENT
        );
        this.clusterService = clusterService;
        this.indicesService = indicesService;
    }

    @Override
    protected boolean resolveIndex(GetFieldAliasesMappingsIndexRequest request) {
        // internal action, index already resolved
        return false;
    }

    @Override
    protected ShardsIterator shards(ClusterState state, InternalRequest request) {
        // Will balance requests between shards
        return state.routingTable().index(request.concreteIndex()).randomAllActiveShardsIt();
    }

    @Override
    protected GetFieldAliasesMappingsResponse shardOperation(final GetFieldAliasesMappingsIndexRequest request, ShardId shardId) {
        assert shardId != null;
        IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        Predicate<String> metadataFieldPredicate = (f) -> indicesService.isMetadataField(f);
        Predicate<String> fieldPredicate = metadataFieldPredicate.or(indicesService.getFieldFilter().apply(shardId.getIndexName()));

        DocumentMapper documentMapper = indexService.mapperService().documentMapper();
        Map<String, FieldAliasesMappingMetadata> fieldMapping = findFieldAliasesMappings(fieldPredicate, documentMapper, request);
        return new GetFieldAliasesMappingsResponse(singletonMap(shardId.getIndexName(), fieldMapping));
    }

    @Override
    protected Writeable.Reader<GetFieldAliasesMappingsResponse> getResponseReader() {
        return GetFieldAliasesMappingsResponse::new;
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, InternalRequest request) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA_READ, request.concreteIndex());
    }

    private static final ToXContent.Params includeDefaultsParams = new ToXContent.Params() {

        static final String INCLUDE_DEFAULTS = "include_defaults";

        @Override
        public String param(String key) {
            if (INCLUDE_DEFAULTS.equals(key)) {
                return "true";
            }
            return null;
        }

        @Override
        public String param(String key, String defaultValue) {
            if (INCLUDE_DEFAULTS.equals(key)) {
                return "true";
            }
            return defaultValue;
        }

        @Override
        public boolean paramAsBoolean(String key, boolean defaultValue) {
            if (INCLUDE_DEFAULTS.equals(key)) {
                return true;
            }
            return defaultValue;
        }

        @Override
        public Boolean paramAsBoolean(String key, Boolean defaultValue) {
            if (INCLUDE_DEFAULTS.equals(key)) {
                return true;
            }
            return defaultValue;
        }
    };

    /**
     * This method searches the field's aliases within the DocumentMapper
     */
    private static Map<String, FieldAliasesMappingMetadata> findFieldAliasesMappings(
        Predicate<String> fieldPredicate,
        DocumentMapper documentMapper,
        GetFieldAliasesMappingsIndexRequest request
    ) {
        if (documentMapper == null) {
            return Collections.emptyMap();
        }
        Map<String, FieldAliasesMappingMetadata> fieldMappings = new HashMap<>();
        final FieldTypeLookup aliasesLookup = documentMapper.fieldTypes();
        final MappingLookup allFieldMappers = documentMapper.mappers();
        for (String field : request.fields()) {
            if (Regex.isMatchAllPattern(field)) {
                for (Mapper fieldMapper : allFieldMappers) {
                    for (Mapper fieldAliasMapper : getFieldAliases(fieldMapper, aliasesLookup, allFieldMappers)) {
                        addFieldMapper(fieldPredicate, fieldAliasMapper.name(), fieldAliasMapper, fieldMappings, request.includeDefaults());
                    }
                }
            } else if (Regex.isSimpleMatchPattern(field)) {
                for (Mapper fieldMapper : allFieldMappers) {
                    if (Regex.simpleMatch(field, fieldMapper.name())) {
                        for (Mapper fieldAliasMapper : getFieldAliases(fieldMapper, aliasesLookup, allFieldMappers)) {
                            addFieldMapper(
                                fieldPredicate,
                                fieldAliasMapper.name(),
                                fieldAliasMapper,
                                fieldMappings,
                                request.includeDefaults()
                            );
                        }
                    }
                }
            } else {
                // not a pattern
                Mapper fieldMapper = allFieldMappers.getMapper(field);
                if (fieldMapper != null) {
                    for (Mapper fieldAliasMapper : getFieldAliases(fieldMapper, aliasesLookup, allFieldMappers)) {
                        addFieldMapper(fieldPredicate, fieldAliasMapper.name(), fieldAliasMapper, fieldMappings, request.includeDefaults());
                    }
                }
            }
        }
        return Collections.unmodifiableMap(fieldMappings);
    }

    /**
     * find all the alias fields which have a path = name
     */
    private static List<Mapper> getFieldAliases(Mapper field, FieldTypeLookup aliasesLookup, MappingLookup mapper) {
        return mapper.getMappers(aliasesLookup.getAliases(field.name()));
    }

    private static void addFieldMapper(
        Predicate<String> fieldPredicate,
        String field,
        Mapper fieldMapper,
        Map<String, FieldAliasesMappingMetadata> fieldMappings,
        boolean includeDefaults
    ) {
        if (fieldMappings.containsKey(field)) {
            return;
        }
        if (fieldPredicate.test(field)) {
            try {
                BytesReference bytes = XContentHelper.toXContent(
                    fieldMapper,
                    MediaTypeRegistry.JSON,
                    includeDefaults ? includeDefaultsParams : ToXContent.EMPTY_PARAMS,
                    false
                );
                fieldMappings.put(field, new FieldAliasesMappingMetadata(fieldMapper.name(), bytes));
            } catch (IOException e) {
                throw new OpenSearchException("failed to serialize XContent of field [" + field + "]", e);
            }
        }
    }
}
