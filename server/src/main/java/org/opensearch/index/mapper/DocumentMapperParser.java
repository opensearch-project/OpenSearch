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

import org.opensearch.Version;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.similarity.SimilarityService;
import org.opensearch.indices.mapper.MapperRegistry;
import org.opensearch.script.ScriptService;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Supplier;

import static java.util.Collections.unmodifiableMap;

/**
 * Parser for a document mapper
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class DocumentMapperParser {

    final MapperService mapperService;
    private final NamedXContentRegistry xContentRegistry;
    private final SimilarityService similarityService;
    private final Supplier<QueryShardContext> queryShardContextSupplier;

    private final RootObjectMapper.TypeParser rootObjectTypeParser = new RootObjectMapper.TypeParser();

    private final Version indexVersionCreated;

    private final Map<String, Mapper.TypeParser> typeParsers;
    private final Map<String, MetadataFieldMapper.TypeParser> rootTypeParsers;
    private final ScriptService scriptService;

    public DocumentMapperParser(
        IndexSettings indexSettings,
        MapperService mapperService,
        NamedXContentRegistry xContentRegistry,
        SimilarityService similarityService,
        MapperRegistry mapperRegistry,
        Supplier<QueryShardContext> queryShardContextSupplier,
        ScriptService scriptService
    ) {
        this.mapperService = mapperService;
        this.xContentRegistry = xContentRegistry;
        this.similarityService = similarityService;
        this.queryShardContextSupplier = queryShardContextSupplier;
        this.scriptService = scriptService;
        this.typeParsers = mapperRegistry.getMapperParsers();
        this.indexVersionCreated = indexSettings.getIndexVersionCreated();
        this.rootTypeParsers = mapperRegistry.getMetadataMapperParsers();
    }

    public Mapper.TypeParser.ParserContext parserContext() {
        return new Mapper.TypeParser.ParserContext(
            similarityService::getSimilarity,
            mapperService,
            typeParsers::get,
            indexVersionCreated,
            queryShardContextSupplier,
            null,
            scriptService
        );
    }

    public Mapper.TypeParser.ParserContext parserContext(DateFormatter dateFormatter) {
        return new Mapper.TypeParser.ParserContext(
            similarityService::getSimilarity,
            mapperService,
            typeParsers::get,
            indexVersionCreated,
            queryShardContextSupplier,
            dateFormatter,
            scriptService
        );
    }

    public DocumentMapper parse(@Nullable String type, CompressedXContent source) throws MapperParsingException {
        Map<String, Object> mapping = null;
        if (source != null) {
            Map<String, Object> root = XContentHelper.convertToMap(source.compressedReference(), true, MediaTypeRegistry.JSON).v2();
            Tuple<String, Map<String, Object>> t = extractMapping(type, root);
            type = t.v1();
            mapping = t.v2();
        }
        if (mapping == null) {
            mapping = new HashMap<>();
        }
        return parse(type, mapping);
    }

    @SuppressWarnings({ "unchecked" })
    public DocumentMapper parse(String type, Map<String, Object> mapping) throws MapperParsingException {
        if (type == null) {
            throw new MapperParsingException("Failed to derive type");
        }
        Mapper.TypeParser.ParserContext parserContext = parserContext();
        // parse RootObjectMapper
        DocumentMapper.Builder docBuilder = new DocumentMapper.Builder(
            (RootObjectMapper.Builder) rootObjectTypeParser.parse(type, mapping, parserContext),
            mapperService
        );
        Iterator<Map.Entry<String, Object>> iterator = mapping.entrySet().iterator();
        // parse DocumentMapper
        while (iterator.hasNext()) {
            Map.Entry<String, Object> entry = iterator.next();
            String fieldName = entry.getKey();
            Object fieldNode = entry.getValue();

            MetadataFieldMapper.TypeParser typeParser = rootTypeParsers.get(fieldName);
            if (typeParser != null) {
                iterator.remove();
                if (false == fieldNode instanceof Map) {
                    throw new IllegalArgumentException("[_parent] must be an object containing [type]");
                }
                Map<String, Object> fieldNodeMap = (Map<String, Object>) fieldNode;
                docBuilder.put(typeParser.parse(fieldName, fieldNodeMap, parserContext));
                fieldNodeMap.remove("type");
                checkNoRemainingFields(fieldName, fieldNodeMap, parserContext.indexVersionCreated());
            }
        }

        Map<String, Object> meta = (Map<String, Object>) mapping.remove("_meta");
        if (meta != null) {
            // It may not be required to copy meta here to maintain immutability
            // but the cost is pretty low here.
            docBuilder.meta(unmodifiableMap(new HashMap<>(meta)));
        }

        checkNoRemainingFields(mapping, parserContext.indexVersionCreated(), "Root mapping definition has unsupported parameters: ");

        return docBuilder.build(mapperService);
    }

    public static void checkNoRemainingFields(String fieldName, Map<?, ?> fieldNodeMap, Version indexVersionCreated) {
        checkNoRemainingFields(
            fieldNodeMap,
            indexVersionCreated,
            "Mapping definition for [" + fieldName + "] has unsupported parameters: "
        );
    }

    public static void checkNoRemainingFields(Map<?, ?> fieldNodeMap, Version indexVersionCreated, String message) {
        if (!fieldNodeMap.isEmpty()) {
            throw new MapperParsingException(message + getRemainingFields(fieldNodeMap));
        }
    }

    private static String getRemainingFields(Map<?, ?> map) {
        StringBuilder remainingFields = new StringBuilder();
        for (Object key : map.keySet()) {
            remainingFields.append(" [").append(key).append(" : ").append(map.get(key)).append("]");
        }
        return remainingFields.toString();
    }

    /**
     * Given an optional type name and mapping definition, returns the type and a normalized form of the mappings.
     * <p>
     * The provided mapping definition may or may not contain the type name as the root key in the map. This method
     * attempts to unwrap the mappings, so that they no longer contain a type name at the root. If no type name can
     * be found, through either the 'type' parameter or by examining the provided mappings, then an exception will be
     * thrown.
     *
     * @param type An optional type name.
     * @param root The mapping definition.
     * @return A tuple of the form (type, normalized mappings).
     */
    @SuppressWarnings({ "unchecked" })
    private Tuple<String, Map<String, Object>> extractMapping(String type, Map<String, Object> root) throws MapperParsingException {
        if (root.size() == 0) {
            if (type != null) {
                return new Tuple<>(type, root);
            } else {
                throw new MapperParsingException("malformed mapping, no type name found");
            }
        }

        String rootName = root.keySet().iterator().next();
        Tuple<String, Map<String, Object>> mapping;
        if (type == null || type.equals(rootName) || mapperService.resolveDocumentType(type).equals(rootName)) {
            mapping = new Tuple<>(rootName, (Map<String, Object>) root.get(rootName));
        } else {
            mapping = new Tuple<>(type, root);
        }
        return mapping;
    }

    NamedXContentRegistry getXContentRegistry() {
        return xContentRegistry;
    }
}
