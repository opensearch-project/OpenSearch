/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MatchOnlyTextFieldMapper;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.TextFieldMapper;

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

    // ── Default factories ──
    private static final LuceneFieldFactory TEXT_FACTORY = (doc, ft, value, lft) -> {
        doc.add(new Field(ft.name(), value.toString(), TextFieldMapper.Defaults.FIELD_TYPE));
    };

    private static final LuceneFieldFactory KEYWORD_FACTORY = (doc, ft, value, lft) -> {
        doc.add(new Field(ft.name(), value.toString(), KeywordFieldMapper.Defaults.FIELD_TYPE));
    };

    private static final LuceneFieldFactory MATCH_ONLY_TEXT_FACTORY = (doc, ft, value, lft) -> {
        doc.add(new Field(ft.name(), value.toString(), TextFieldMapper.Defaults.FIELD_TYPE));
    };

    private static final LuceneFieldFactory ID_FIELD_FACTORY = (doc, ft, value, lft) -> {
        doc.add(new Field(ft.name(), new BytesRef((byte[]) value), IdFieldMapper.Defaults.FIELD_TYPE));
    };

    private static final LuceneFieldFactory SEQ_NO_FIELD_FACTORY = (doc, ft, value, lft) -> {
        doc.add(new LongPoint(ft.name(), (long) value));
    };

    // ── Registry ──

    private final Map<String, LuceneFieldFactory> factories = new ConcurrentHashMap<>();

    /**
     * Creates a registry pre-populated with the default full-text-searchable field factories.
     */
    public LuceneFieldFactoryRegistry() {
        register(TextFieldMapper.CONTENT_TYPE, TEXT_FACTORY);
        register(KeywordFieldMapper.CONTENT_TYPE, KEYWORD_FACTORY);
        register(MatchOnlyTextFieldMapper.CONTENT_TYPE, MATCH_ONLY_TEXT_FACTORY);
        register(IdFieldMapper.CONTENT_TYPE, ID_FIELD_FACTORY);
        register(SeqNoFieldMapper.CONTENT_TYPE, SEQ_NO_FIELD_FACTORY);
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
