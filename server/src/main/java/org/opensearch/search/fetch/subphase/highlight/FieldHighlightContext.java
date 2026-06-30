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

package org.opensearch.search.fetch.subphase.highlight;

import org.apache.lucene.search.Query;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.search.fetch.FetchContext;
import org.opensearch.search.fetch.FetchSubPhase;

import java.util.Map;

/**
 * Context used during field level highlighting
 *
 * @opensearch.internal
 */
public class FieldHighlightContext {

    public final String fieldName;
    public final SearchHighlightContext.Field field;
    public final MappedFieldType fieldType;
    public final FetchContext context;
    public final FetchSubPhase.HitContext hitContext;
    public final Query query;
    public final boolean forceSource;
    public final Map<String, Object> cache;

    public FieldHighlightContext(
        String fieldName,
        SearchHighlightContext.Field field,
        MappedFieldType fieldType,
        FetchContext context,
        FetchSubPhase.HitContext hitContext,
        Query query,
        boolean forceSource,
        Map<String, Object> cache
    ) {
        this.fieldName = fieldName;
        this.field = field;
        this.fieldType = fieldType;
        this.context = context;
        this.hitContext = hitContext;
        this.query = query;
        this.forceSource = forceSource;
        this.cache = cache;
    }
}
