/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry of {@link LuceneFieldFactory} instances keyed by OpenSearch field type name.
 *
 * Provides a default registry pre-populated with factories for the standard full-text-searchable
 * types ({@code text}, {@code keyword}, {@code match_only_text}). Additional types can be
 * registered at runtime via {@link #register(String, LuceneFieldFactory)}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class LuceneFieldFactoryRegistry {

    // ── Pre-built Lucene FieldTypes matching OpenSearch mapper defaults ──

    /** Matches {@code TextFieldMapper.Defaults.FIELD_TYPE}. */
    private static final FieldType TEXT_FIELD_TYPE;
    static {
        TEXT_FIELD_TYPE = new FieldType();
        TEXT_FIELD_TYPE.setTokenized(true);
        TEXT_FIELD_TYPE.setStored(false);
        TEXT_FIELD_TYPE.setStoreTermVectors(false);
        TEXT_FIELD_TYPE.setOmitNorms(false);
        TEXT_FIELD_TYPE.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
        TEXT_FIELD_TYPE.freeze();
    }

    /** Matches {@code KeywordFieldMapper.Defaults.FIELD_TYPE}. */
    private static final FieldType KEYWORD_FIELD_TYPE;
    static {
        KEYWORD_FIELD_TYPE = new FieldType();
        KEYWORD_FIELD_TYPE.setTokenized(false);
        KEYWORD_FIELD_TYPE.setStored(false);
        KEYWORD_FIELD_TYPE.setOmitNorms(true);
        KEYWORD_FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
        KEYWORD_FIELD_TYPE.freeze();
    }

    /** Matches {@code MatchOnlyTextFieldMapper.FIELD_TYPE}. */
    private static final FieldType MATCH_ONLY_TEXT_FIELD_TYPE;
    static {
        MATCH_ONLY_TEXT_FIELD_TYPE = new FieldType();
        MATCH_ONLY_TEXT_FIELD_TYPE.setTokenized(true);
        MATCH_ONLY_TEXT_FIELD_TYPE.setStored(false);
        MATCH_ONLY_TEXT_FIELD_TYPE.setStoreTermVectors(false);
        MATCH_ONLY_TEXT_FIELD_TYPE.setOmitNorms(true);
        MATCH_ONLY_TEXT_FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
        MATCH_ONLY_TEXT_FIELD_TYPE.freeze();
    }

    // ── Default factories ──

    private static final LuceneFieldFactory TEXT_FACTORY = (doc, ft, value) -> {
        doc.add(new Field(ft.name(), value.toString(), TEXT_FIELD_TYPE));
    };

    private static final LuceneFieldFactory KEYWORD_FACTORY = (doc, ft, value) -> {
        BytesRef binaryValue = new BytesRef(value.toString());
        doc.add(new Field(ft.name(), binaryValue, KEYWORD_FIELD_TYPE));
    };

    private static final LuceneFieldFactory MATCH_ONLY_TEXT_FACTORY = (doc, ft, value) -> {
        doc.add(new Field(ft.name(), value.toString(), MATCH_ONLY_TEXT_FIELD_TYPE));
    };

    // ── Registry ──

    private final Map<String, LuceneFieldFactory> factories = new ConcurrentHashMap<>();

    /**
     * Creates a registry pre-populated with the default full-text-searchable field factories.
     */
    public LuceneFieldFactoryRegistry() {
        factories.put("text", TEXT_FACTORY);
        factories.put("keyword", KEYWORD_FACTORY);
        factories.put("match_only_text", MATCH_ONLY_TEXT_FACTORY);
    }

    /**
     * Registers a factory for the given field type name. Overwrites any existing registration.
     *
     * @param typeName the OpenSearch field type name (e.g., "text", "keyword")
     * @param factory  the factory that creates Lucene fields for this type
     */
    public void register(String typeName, LuceneFieldFactory factory) {
        factories.put(typeName, factory);
    }

    /**
     * Returns the factory for the given type name, or {@code null} if not registered.
     *
     * @param typeName the OpenSearch field type name
     * @return the factory, or null
     */
    public LuceneFieldFactory get(String typeName) {
        return factories.get(typeName);
    }

    /**
     * Returns the set of currently registered type names.
     *
     * @return unmodifiable set of supported type names
     */
    public Set<String> supportedTypes() {
        return Set.copyOf(factories.keySet());
    }
}
