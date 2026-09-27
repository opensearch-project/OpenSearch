/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.query;

import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;

import java.util.Optional;

/**
 * Translator mapper that unconditionally rejects range and term queries. Used for field types
 * that share a {@code SqlTypeName} with a supported UDT but are not themselves supported
 * (e.g. plain VARBINARY binary fields which share the type name with IpType).
 *
 * <p>The rejection message mirrors legacy {@code MappedFieldType.rangeQuery} at line 216:
 * "Field [name] of type [type] does not support range queries".
 *
 * <p>Stateless singleton; no per-field state.
 */
final class RejectingTranslatorMapper extends BaseTranslatorMapper {

    /** Singleton instance. */
    static final RejectingTranslatorMapper INSTANCE = new RejectingTranslatorMapper();

    private RejectingTranslatorMapper() {}

    /**
     * Always throws, rejecting range queries on this field type.
     *
     * @throws ConversionException always
     */
    @Override
    protected RexNode translateBound(Object value, boolean isLower, boolean inclusive, RelDataTypeField field, ConversionContext ctx)
        throws ConversionException {
        throw new ConversionException(
            "Field [" + field.getName() + "] of type [" + field.getType().getSqlTypeName() + "] does not support range queries"
        );
    }

    /**
     * Always throws, rejecting term queries on this field type.
     *
     * @throws ConversionException always
     */
    @Override
    public Optional<RexNode> toTermLiteral(Object value, RelDataTypeField field, ConversionContext ctx) throws ConversionException {
        throw new ConversionException(
            "Field [" + field.getName() + "] of type [" + field.getType().getSqlTypeName() + "] does not support term queries"
        );
    }
}
