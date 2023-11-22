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

package org.opensearch.search.lookup;

import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;

import java.util.function.Function;

/**
 * Looks up a document
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class DocLookup {

    private final MapperService mapperService;
    private final Function<MappedFieldType, IndexFieldData<?>> fieldDataLookup;

    DocLookup(MapperService mapperService, Function<MappedFieldType, IndexFieldData<?>> fieldDataLookup) {
        this.mapperService = mapperService;
        this.fieldDataLookup = fieldDataLookup;
    }

    public MapperService mapperService() {
        return this.mapperService;
    }

    public IndexFieldData<?> getForField(MappedFieldType fieldType) {
        return fieldDataLookup.apply(fieldType);
    }

    public LeafDocLookup getLeafDocLookup(LeafReaderContext context) {
        return new LeafDocLookup(mapperService, fieldDataLookup, context);
    }
}
