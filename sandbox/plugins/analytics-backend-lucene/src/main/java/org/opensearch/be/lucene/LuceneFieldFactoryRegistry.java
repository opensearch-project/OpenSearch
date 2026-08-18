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
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.mapper.FlatObjectFieldMapper;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MatchOnlyTextFieldMapper;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.SourceFieldMapper;
import org.opensearch.index.mapper.TextFieldMapper;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
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

    private static final FieldType ID_FIELD_TYPE = new FieldType();

    static {
        ID_FIELD_TYPE.setTokenized(false);
        ID_FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
        ID_FIELD_TYPE.setOmitNorms(true);
        ID_FIELD_TYPE.setStored(false);
        ID_FIELD_TYPE.setDocValuesType(DocValuesType.NONE);
        ID_FIELD_TYPE.freeze();
    }

    // ── Default factories ──
    private static final LuceneFieldFactory TEXT_FACTORY = (doc, ft, value, lft) -> {
        doc.add(new Field(ft.name(), value.toString(), lft));
    };

    private static final LuceneFieldFactory KEYWORD_FACTORY = (doc, ft, value, lft) -> {
        doc.add(new Field(ft.name(), value.toString(), lft));
    };

    private static final LuceneFieldFactory MATCH_ONLY_TEXT_FACTORY = (doc, ft, value, lft) -> {
        doc.add(new Field(ft.name(), value.toString(), lft));
    };

    private static final LuceneFieldFactory ID_FIELD_FACTORY = (doc, ft, value, lft) -> {
        doc.add(new Field(ft.name(), new BytesRef((byte[]) value), ID_FIELD_TYPE));
    };

    /**
     * Indexes a {@code flat_object} field's leaves as searchable terms.
     *
     * <p>The value is the ordered {@code (relative path, value)} entry list the mapper produces for a
     * pluggable data format — the whole object arrives in one call rather than one call per leaf. This
     * expands it into the same three Lucene fields the non-pluggable path writes, because those are
     * exactly what {@code FlatObjectFieldType}'s queries resolve to:
     * <ul>
     *   <li>{@code <field>._value} ← the bare leaf value, for a query on the field itself.</li>
     *   <li>{@code <field>._valueAndPath} ← {@code <field>.<path>=<value>}, for a dotted-path query
     *       such as {@code LogAttributes.http.status: 500}, which rewrites to that term.</li>
     *   <li>{@code <field>} ← {@code <field>.<pathPart>} per path segment, which is what makes the
     *       field and its sub-paths report as existing.</li>
     * </ul>
     *
     * <p>Only terms are written: doc values stay with the primary format, and {@code lft} already has
     * {@code DocValuesType.NONE} whenever this format did not claim {@code COLUMNAR_STORAGE}. So the
     * doc-values-prefixed form the non-pluggable path also writes is intentionally not produced here.
     */
    private static final LuceneFieldFactory FLAT_OBJECT_FACTORY = (doc, ft, value, lft) -> {
        if (value instanceof List<?> == false) {
            throw new IllegalArgumentException(
                "flat_object field ["
                    + ft.name()
                    + "] expects the mapper's leaf entry list, but got ["
                    + value.getClass().getSimpleName()
                    + "]"
            );
        }
        final String fieldName = ft.name();
        final Set<String> pathParts = new HashSet<>();
        for (Object element : (List<?>) value) {
            if (element instanceof Map.Entry<?, ?> entry) {
                // A null leaf value is dropped by the mapper before it reaches here; guard anyway so
                // a future change cannot silently index the string "null".
                if (entry.getValue() == null) {
                    continue;
                }
                final String relativePath = entry.getKey().toString();
                final String leafValue = entry.getValue().toString();
                final String leafPath = fieldName + "." + relativePath;

                doc.add(new Field(fieldName + "._value", new BytesRef(leafValue), lft));
                doc.add(new Field(fieldName + "._valueAndPath", new BytesRef(leafPath + "=" + leafValue), lft));
                pathParts.addAll(Arrays.asList(relativePath.split("\\.")));
            } else {
                throw new IllegalArgumentException(
                    "flat_object field [" + fieldName + "] expects Map.Entry leaves, but got [" + element + "]"
                );
            }
        }
        // Deduplicated, mirroring the non-pluggable path: the parent field carries the set of path
        // parts, not one term per leaf occurrence.
        for (String part : pathParts) {
            doc.add(new Field(fieldName, new BytesRef(fieldName + "." + part), lft));
        }
    };

    private static final LuceneFieldFactory SEQ_NO_FIELD_FACTORY = (doc, ft, value, lft) -> {
        // do nothing for now since we don't want to index seq no indexing without soft deletes enabled.
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
        register(FlatObjectFieldMapper.CONTENT_TYPE, FLAT_OBJECT_FACTORY);
        registerMetaFields();
    }

    private void registerMetaFields() {
        register(IdFieldMapper.CONTENT_TYPE, ID_FIELD_FACTORY);
        register(SeqNoFieldMapper.CONTENT_TYPE, SEQ_NO_FIELD_FACTORY);
        register(SeqNoFieldMapper.PRIMARY_TERM_NAME, (d, ft, v, lft) -> d.add(new SortedNumericDocValuesField(ft.name(), (long) v)));
        register(SourceFieldMapper.CONTENT_TYPE, (d, ft, v, lft) -> d.add(new Field(ft.name(), (BytesRef) v, lft)));
        // pending routing and ignored field handling
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
