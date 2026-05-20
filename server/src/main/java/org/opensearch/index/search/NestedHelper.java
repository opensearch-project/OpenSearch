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

package org.opensearch.index.search;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.ObjectMapper;
import org.opensearch.search.approximate.ApproximateScoreQuery;

/** Utility class to filter parent and children clauses when building nested
 * queries.
 *
 * @opensearch.internal
 */
public final class NestedHelper {

    private final MapperService mapperService;

    public NestedHelper(MapperService mapperService) {
        this.mapperService = mapperService;
    }

    /** Returns true if the given query might match nested documents. */
    public boolean mightMatchNestedDocs(Query query) {
        return switch (query) {
            case ConstantScoreQuery csq -> mightMatchNestedDocs(csq.getQuery());
            case BoostQuery bq -> mightMatchNestedDocs(bq.getQuery());
            case MatchAllDocsQuery ignored -> true;
            case MatchNoDocsQuery ignored -> false;
            case TermQuery tq -> mightMatchNestedDocs(tq.getTerm().field());
            case TermInSetQuery tisq -> tisq.getTermsCount() > 0 && mightMatchNestedDocs(tisq.getField());
            case PointRangeQuery prq -> mightMatchNestedDocs(prq.getField());
            case IndexOrDocValuesQuery iorvq -> mightMatchNestedDocs(iorvq.getIndexQuery());
            case ApproximateScoreQuery asq -> mightMatchNestedDocs(asq.getOriginalQuery());
            case BooleanQuery bq -> {
                final boolean hasRequiredClauses = bq.clauses().stream().anyMatch(BooleanClause::isRequired);
                if (hasRequiredClauses) {
                    yield bq.clauses()
                        .stream()
                        .filter(BooleanClause::isRequired)
                        .map(BooleanClause::query)
                        .allMatch(this::mightMatchNestedDocs);
                } else {
                    yield bq.clauses()
                        .stream()
                        .filter(c -> c.occur() == Occur.SHOULD)
                        .map(BooleanClause::query)
                        .anyMatch(this::mightMatchNestedDocs);
                }
            }
            case OpenSearchToParentBlockJoinQuery opbq -> opbq.getPath() != null;
            case null, default -> true;
        };
    }

    /** Returns true if a query on the given field might match nested documents. */
    boolean mightMatchNestedDocs(String field) {
        if (field.startsWith("_")) {
            // meta field. Every meta field behaves differently, eg. nested
            // documents have the same _uid as their parent, put their path in
            // the _type field but do not have _field_names. So we just ignore
            // meta fields and return true, which is always safe, it just means
            // we might add a nested filter when it is nor required.
            return true;
        }
        if (mapperService.fieldType(field) == null) {
            // field does not exist
            return false;
        }
        for (String parent = parentObject(field); parent != null; parent = parentObject(parent)) {
            ObjectMapper mapper = mapperService.getObjectMapper(parent);
            if (mapper != null && mapper.nested().isNested()) {
                return true;
            }
        }
        return false;
    }

    /** Returns true if the given query might match parent documents or documents
     *  that are nested under a different path. */
    public boolean mightMatchNonNestedDocs(Query query, String nestedPath) {
        return switch (query) {
            case ConstantScoreQuery csq -> mightMatchNonNestedDocs(csq.getQuery(), nestedPath);
            case BoostQuery bq -> mightMatchNonNestedDocs(bq.getQuery(), nestedPath);
            case MatchAllDocsQuery ignored -> true;
            case MatchNoDocsQuery ignored -> false;
            case TermQuery tq -> mightMatchNonNestedDocs(tq.getTerm().field(), nestedPath);
            case TermInSetQuery tisq -> tisq.getTermsCount() > 0 && mightMatchNonNestedDocs(tisq.getField(), nestedPath);
            case PointRangeQuery prq -> mightMatchNonNestedDocs(prq.getField(), nestedPath);
            case IndexOrDocValuesQuery iorvq -> mightMatchNonNestedDocs(iorvq.getIndexQuery(), nestedPath);
            case ApproximateScoreQuery asq -> mightMatchNonNestedDocs(asq.getOriginalQuery(), nestedPath);
            case BooleanQuery bq -> {
                final boolean hasRequiredClauses = bq.clauses().stream().anyMatch(BooleanClause::isRequired);
                if (hasRequiredClauses) {
                    yield bq.clauses()
                        .stream()
                        .filter(BooleanClause::isRequired)
                        .map(BooleanClause::query)
                        .allMatch(q -> mightMatchNonNestedDocs(q, nestedPath));
                } else {
                    yield bq.clauses()
                        .stream()
                        .filter(c -> c.occur() == Occur.SHOULD)
                        .map(BooleanClause::query)
                        .anyMatch(q -> mightMatchNonNestedDocs(q, nestedPath));
                }
            }
            case null, default -> true;
        };
    }

    /** Returns true if a query on the given field might match parent documents
     *  or documents that are nested under a different path. */
    boolean mightMatchNonNestedDocs(String field, String nestedPath) {
        if (field.startsWith("_")) {
            // meta field. Every meta field behaves differently, eg. nested
            // documents have the same _uid as their parent, put their path in
            // the _type field but do not have _field_names. So we just ignore
            // meta fields and return true, which is always safe, it just means
            // we might add a nested filter when it is nor required.
            return true;
        }
        if (mapperService.fieldType(field) == null) {
            return false;
        }
        for (String parent = parentObject(field); parent != null; parent = parentObject(parent)) {
            ObjectMapper mapper = mapperService.getObjectMapper(parent);
            if (mapper != null && mapper.nested().isNested()) {
                if (mapper.fullPath().equals(nestedPath)) {
                    // If the mapper does not include in its parent or in the root object then
                    // the query might only match nested documents with the given path
                    return mapper.nested().isIncludeInParent() || mapper.nested().isIncludeInRoot();
                } else {
                    // the first parent nested mapper does not have the expected path
                    // It might be misconfiguration or a sub nested mapper
                    return true;
                }
            }
        }
        return true; // the field is not a sub field of the nested path
    }

    private static String parentObject(String field) {
        int lastDot = field.lastIndexOf('.');
        if (lastDot == -1) {
            return null;
        }
        return field.substring(0, lastDot);
    }

}
