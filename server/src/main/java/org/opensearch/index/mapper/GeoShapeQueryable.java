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

import org.apache.lucene.search.Query;
import org.opensearch.common.geo.ShapeRelation;
import org.opensearch.common.geo.SpatialStrategy;
import org.opensearch.geometry.Geometry;
import org.opensearch.index.query.QueryShardContext;

/**
 * Implemented by {@link org.opensearch.index.mapper.MappedFieldType} that support
 * GeoShape queries.
 *
 * @opensearch.internal
 */
public interface GeoShapeQueryable {

    Query geoShapeQuery(Geometry shape, String fieldName, ShapeRelation relation, QueryShardContext context);

    @Deprecated
    default Query geoShapeQuery(
        Geometry shape,
        String fieldName,
        SpatialStrategy strategy,
        ShapeRelation relation,
        QueryShardContext context
    ) {
        return geoShapeQuery(shape, fieldName, relation, context);
    }
}
