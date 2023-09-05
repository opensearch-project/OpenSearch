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

import org.opensearch.index.mapper.MetadataFieldMapper.TypeParser;
import org.opensearch.index.query.RankFeatureQueryBuilder;
import org.opensearch.plugins.MapperPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchPlugin;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class MapperExtrasPlugin extends Plugin implements MapperPlugin, SearchPlugin {

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        Map<String, Mapper.TypeParser> mappers = new LinkedHashMap<>();
        mappers.put(ScaledFloatFieldMapper.CONTENT_TYPE, ScaledFloatFieldMapper.PARSER);
        mappers.put(TokenCountFieldMapper.CONTENT_TYPE, TokenCountFieldMapper.PARSER);
        mappers.put(RankFeatureFieldMapper.CONTENT_TYPE, RankFeatureFieldMapper.PARSER);
        mappers.put(RankFeaturesFieldMapper.CONTENT_TYPE, RankFeaturesFieldMapper.PARSER);
        mappers.put(SearchAsYouTypeFieldMapper.CONTENT_TYPE, SearchAsYouTypeFieldMapper.PARSER);
        return Collections.unmodifiableMap(mappers);
    }

    @Override
    public Map<String, TypeParser> getMetadataMappers() {
        return Collections.singletonMap(RankFeatureMetaFieldMapper.CONTENT_TYPE, RankFeatureMetaFieldMapper.PARSER);
    }

    @Override
    public List<QuerySpec<?>> getQueries() {
        return Collections.singletonList(
            new QuerySpec<>(RankFeatureQueryBuilder.NAME, RankFeatureQueryBuilder::new, p -> RankFeatureQueryBuilder.PARSER.parse(p, null))
        );
    }

}
