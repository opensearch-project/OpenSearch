/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.index.IndexCreationValidator;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.DocumentMapper;
import org.opensearch.index.mapper.FieldMapper;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.ObjectMapper;

/**
 * Validates that a {@code nested} object's mapping shape is representable on a composite (pluggable
 * data format) index, where fields declared directly inside a {@code nested} object are persisted as a
 * coarse, doc-values-only projection in the secondary format(s) and materialized as flat leaves /
 * {@code flat_object} MAPs in the primary (Parquet) format. Two shapes are rejected at index-creation
 * time — and again on every mapping update, which runs the same validator — instead of being
 * silently mis-indexed:
 * <ol>
 *   <li>a {@code nested} object must set {@code dynamic: false} or {@code dynamic: strict} — an
 *       undeclared leaf can silently vanish from the primary format while still reaching a secondary's
 *       coarse projection;</li>
 *   <li>no plain (non-nested) {@code object} sub-field may appear under a {@code nested} object's scope
 *       — including via a dotted field name (e.g. {@code "meta.name"}), which implicitly builds the same
 *       disallowed intermediate object mapper;</li>
 *   <li>no {@code nested} sub-field may appear under a {@code nested} object's scope — nested objects
 *       cannot contain further nested objects in this storage mode; and</li>
 *   <li>no leaf (or multi-field) under a {@code nested} object's scope may resolve to
 *       {@code multi_value} {@code LIST} storage — a nested element's struct child is single-valued.
 *       The {@code AUTO} state passes at creation; its promotion to {@code LIST} is a mapping update,
 *       which re-runs this validator and is rejected then.</li>
 * </ol>
 * <p>
 * {@code index} is deliberately not validated here: a nested-scope leaf that resolves to
 * {@code index: true} requests a search-shaped capability no format can serve inside nested, which the
 * capability coverage check in {@code CompositeDataFormatPlugin#assignCapabilities} rejects with an
 * error directing the user to set {@code index: false}.
 *
 * @opensearch.experimental
 */
public class CompositeIndexCreationValidator implements IndexCreationValidator {

    /** Matches {@code IndexSettings#isPluggableDataFormatEnabled}'s underlying setting key. */
    static final String PLUGGABLE_DATAFORMAT_ENABLED_SETTING = "index.pluggable.dataformat.enabled";

    @Override
    public void validate(MapperService mapperService, IndexSettings indexSettings) {
        boolean pluggableDataFormatEnabled = indexSettings.getSettings().getAsBoolean(PLUGGABLE_DATAFORMAT_ENABLED_SETTING, false);
        if (pluggableDataFormatEnabled == false) {
            return;
        }
        DocumentMapper documentMapper = mapperService.documentMapper();
        if (documentMapper == null) {
            return;
        }
        validate(documentMapper.root(), false);
    }

    /**
     * Recursively validates {@code mapper}'s children. {@code insideNestedScope} is {@code true} once
     * recursion has passed through a {@code nested} object mapper, and stays {@code true} for every
     * descendant from there on. (Within a nested scope no object mapper of any kind — plain or nested —
     * is permitted, so in practice recursion below a nested mapper only ever checks its direct leaves.)
     */
    private void validate(ObjectMapper mapper, boolean insideNestedScope) {
        boolean nested = mapper.nested().isNested();
        if (nested) {
            ObjectMapper.Dynamic dynamic = mapper.dynamic();
            if (dynamic != ObjectMapper.Dynamic.FALSE && dynamic != ObjectMapper.Dynamic.STRICT) {
                throw new IllegalArgumentException(
                    "Nested field ["
                        + mapper.simpleName()
                        + "] must set [dynamic: false] or [dynamic: strict] on composite (pluggable data "
                        + "format) indices; a dynamically-mapped leaf inside nested cannot be reliably "
                        + "represented in this storage mode."
                );
            }
        }
        boolean childScope = insideNestedScope || nested;
        for (Mapper child : mapper) {
            if (child instanceof ObjectMapper childObject) {
                if (childScope && childObject.nested().isNested()) {
                    throw new IllegalArgumentException(
                        "Nested field ["
                            + childObject.simpleName()
                            + "] inside nested field ["
                            + mapper.simpleName()
                            + "] is not supported on composite (pluggable data format) indices; "
                            + "nested objects cannot contain further nested objects."
                    );
                }
                if (childScope) {
                    throw new IllegalArgumentException(
                        "Object field ["
                            + childObject.simpleName()
                            + "] inside nested field ["
                            + mapper.simpleName()
                            + "] is not supported on composite (pluggable data format) indices; "
                            + "use flat fields or a [flat_object] field instead."
                    );
                }
                validate(childObject, childScope);
            } else if (childScope && child instanceof FieldMapper fieldMapper) {
                checkNoMultiValue(fieldMapper, mapper.simpleName());
            }
        }
    }

    /**
     * Rejects a nested-scope leaf (or any of its multi-fields) that resolves to {@code multi_value}
     * {@code LIST} storage: a nested element's struct child is single-valued, and the {@code LIST}
     * write path does not exist inside a nested {@code LIST<STRUCT>} column. The {@code AUTO} state
     * passes here by design — its promotion to {@code LIST} is a mapping update, which re-runs this
     * validator and is rejected at promotion time.
     */
    private void checkNoMultiValue(FieldMapper fieldMapper, String nestedFieldName) {
        if (fieldMapper.fieldType().isMultiValued()) {
            throw new IllegalArgumentException(
                "Field ["
                    + fieldMapper.simpleName()
                    + "] inside nested field ["
                    + nestedFieldName
                    + "] sets [multi_value: true], which is not supported on composite (pluggable data "
                    + "format) indices; fields within a nested object are single-valued per element."
            );
        }
        for (Mapper sub : fieldMapper) {
            if (sub instanceof FieldMapper subField) {
                checkNoMultiValue(subField, nestedFieldName);
            }
        }
    }
}
