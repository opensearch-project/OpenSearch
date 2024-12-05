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

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * Orchestrator class for search phase lookups
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class SearchLookup {
    /**
     * The maximum depth of field dependencies.
     * When a runtime field's doc values depends on another runtime field's doc values,
     * which depends on another runtime field's doc values and so on, it can
     * make a very deep stack, which we want to limit.
     */
    private static final int MAX_FIELD_CHAIN_DEPTH = 5;

    /**
     * This constant should be used in cases when shard id is unknown.
     * Mostly it should be used in tests.
     */
    public static final int UNKNOWN_SHARD_ID = -1;

    /**
     * The chain of fields for which this lookup was created, used for detecting
     * loops caused by runtime fields referring to other runtime fields. The chain is empty
     * for the "top level" lookup created for the entire search. When a lookup is used to load
     * fielddata for a field, we fork it and make sure the field name name isn't in the chain,
     * then add it to the end. So the lookup for the a field named {@code a} will be {@code ["a"]}. If
     * that field looks up the values of a field named {@code b} then
     * {@code b}'s chain will contain {@code ["a", "b"]}.
     */
    private final Set<String> fieldChain;
    private final DocLookup docMap;
    private final SourceLookup sourceLookup;
    private final FieldsLookup fieldsLookup;
    private final BiFunction<MappedFieldType, Supplier<SearchLookup>, IndexFieldData<?>> fieldDataLookup;
    private final int shardId;

    /**
     * Constructor for backwards compatibility. Use the one with explicit shardId argument.
     */
    @Deprecated
    public SearchLookup(
        MapperService mapperService,
        BiFunction<MappedFieldType, Supplier<SearchLookup>, IndexFieldData<?>> fieldDataLookup
    ) {
        this(mapperService, fieldDataLookup, UNKNOWN_SHARD_ID);
    }

    /**
     * Create the top level field lookup for a search request. Provides a way to look up fields from doc_values,
     * stored fields, or _source.
     */
    public SearchLookup(
        MapperService mapperService,
        BiFunction<MappedFieldType, Supplier<SearchLookup>, IndexFieldData<?>> fieldDataLookup,
        int shardId
    ) {
        this.fieldChain = Collections.emptySet();
        docMap = new DocLookup(
            mapperService,
            fieldType -> fieldDataLookup.apply(fieldType, () -> forkAndTrackFieldReferences(fieldType.name()))
        );
        sourceLookup = new SourceLookup();
        fieldsLookup = new FieldsLookup(mapperService);
        this.fieldDataLookup = fieldDataLookup;
        this.shardId = shardId;
    }

    /**
     * Create a new {@link SearchLookup} that looks fields up the same as the one provided as argument,
     * while also tracking field references starting from the provided field name. It detects cycles
     * and prevents resolving fields that depend on more than {@link #MAX_FIELD_CHAIN_DEPTH} fields.
     * @param searchLookup the existing lookup to create a new one from
     * @param fieldChain the chain of fields that required the field currently being loaded
     */
    private SearchLookup(SearchLookup searchLookup, Set<String> fieldChain) {
        this.fieldChain = Collections.unmodifiableSet(fieldChain);
        this.docMap = new DocLookup(
            searchLookup.docMap.mapperService(),
            fieldType -> searchLookup.fieldDataLookup.apply(fieldType, () -> forkAndTrackFieldReferences(fieldType.name()))
        );
        this.sourceLookup = searchLookup.sourceLookup;
        this.fieldsLookup = searchLookup.fieldsLookup;
        this.fieldDataLookup = searchLookup.fieldDataLookup;
        this.shardId = searchLookup.shardId;
    }

    /**
     * Creates a copy of the current {@link SearchLookup} that looks fields up in the same way, but also tracks field references
     * in order to detect cycles and prevent resolving fields that depend on more than {@link #MAX_FIELD_CHAIN_DEPTH} other fields.
     * @param field the field being referred to, for which fielddata needs to be loaded
     * @return the new lookup
     * @throws IllegalArgumentException if a cycle is detected in the fields required to build doc values, or if the field
     * being resolved depends on more than {@link #MAX_FIELD_CHAIN_DEPTH}
     */
    public final SearchLookup forkAndTrackFieldReferences(String field) {
        Objects.requireNonNull(field, "field cannot be null");
        Set<String> newFieldChain = new LinkedHashSet<>(fieldChain);
        if (newFieldChain.add(field) == false) {
            String message = String.join(" -> ", newFieldChain) + " -> " + field;
            throw new IllegalArgumentException("Cyclic dependency detected while resolving runtime fields: " + message);
        }
        if (newFieldChain.size() > MAX_FIELD_CHAIN_DEPTH) {
            throw new IllegalArgumentException("Field requires resolving too many dependent fields: " + String.join(" -> ", newFieldChain));
        }
        return new SearchLookup(this, newFieldChain);
    }

    /**
     * SourceLookup is not thread safe, so we create a new instance for each leaf to support concurrent segment search
     */
    public LeafSearchLookup getLeafSearchLookup(LeafReaderContext context) {
        return new LeafSearchLookup(
            context,
            docMap.getLeafDocLookup(context),
            new SourceLookup(),
            fieldsLookup.getLeafFieldsLookup(context)
        );
    }

    public DocLookup doc() {
        return docMap;
    }

    /**
     * Returned SourceLookup will be unrelated to any created LeafSearchLookups. Instead, use {@link LeafSearchLookup#source()} to access the related {@link SearchLookup}.
     */
    public SourceLookup source() {
        return sourceLookup;
    }

    public int shardId() {
        if (shardId == UNKNOWN_SHARD_ID) {
            throw new IllegalStateException("Shard id is unknown for this lookup");
        }
        return shardId;
    }
}
