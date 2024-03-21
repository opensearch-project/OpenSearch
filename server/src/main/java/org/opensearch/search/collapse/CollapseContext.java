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

package org.opensearch.search.collapse;

import org.apache.lucene.search.Sort;
import org.apache.lucene.search.grouping.CollapsingTopDocsCollector;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.query.InnerHitBuilder;

import java.util.List;

/**
 * Context used for field collapsing
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class CollapseContext {
    private final String fieldName;
    private final MappedFieldType fieldType;
    private final List<InnerHitBuilder> innerHits;

    public CollapseContext(String fieldName, MappedFieldType fieldType, List<InnerHitBuilder> innerHits) {
        this.fieldName = fieldName;
        this.fieldType = fieldType;
        this.innerHits = innerHits;
    }

    /**
     * The requested field name to collapse on.
     */
    public String getFieldName() {
        return fieldName;
    }

    /** The field type used for collapsing **/
    public MappedFieldType getFieldType() {
        return fieldType;
    }

    /** The inner hit options to expand the collapsed results **/
    public List<InnerHitBuilder> getInnerHit() {
        return innerHits;
    }

    public CollapsingTopDocsCollector<?> createTopDocs(Sort sort, int topN) {
        if (fieldType instanceof KeywordFieldMapper.KeywordFieldType) {
            return CollapsingTopDocsCollector.createKeyword(fieldName, fieldType, sort, topN);
        } else if (fieldType instanceof NumberFieldMapper.NumberFieldType) {
            return CollapsingTopDocsCollector.createNumeric(fieldName, fieldType, sort, topN);
        } else {
            throw new IllegalStateException("unknown type for collapse field " + fieldName + ", only keywords and numbers are accepted");
        }
    }
}
