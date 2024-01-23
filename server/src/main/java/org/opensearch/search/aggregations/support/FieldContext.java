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

package org.opensearch.search.aggregations.support;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.mapper.MappedFieldType;

/**
 * Used by all field data based aggregators. This determine the context of the field data the aggregators are operating
 * in. It holds both the field names and the index field datas that are associated with them.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class FieldContext {

    private final String field;
    private final IndexFieldData<?> indexFieldData;
    private final MappedFieldType fieldType;

    /**
     * Constructs a field data context for the given field and its index field data
     *
     * @param field             The name of the field
     * @param indexFieldData    The index field data of the field
     */
    public FieldContext(String field, IndexFieldData<?> indexFieldData, MappedFieldType fieldType) {
        this.field = field;
        this.indexFieldData = indexFieldData;
        this.fieldType = fieldType;
    }

    public String field() {
        return field;
    }

    /**
     * @return The index field datas in this context
     */
    public IndexFieldData<?> indexFieldData() {
        return indexFieldData;
    }

    public MappedFieldType fieldType() {
        return fieldType;
    }

}
