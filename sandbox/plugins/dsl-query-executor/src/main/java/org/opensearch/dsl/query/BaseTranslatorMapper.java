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
 * Abstract base for per-field-type translation of range bounds and term values to Calcite RexNodes.
 * Mirrors the legacy {@code MappedFieldType} (abstract class, not interface) hierarchy where
 * {@code rangeQuery} defaults to throw and {@code termQuery} is abstract.
 *
 * <p>Mappers are stateless singletons shared across all fields of a given type in the schema.
 * Per-field state (e.g. scaling factor) is read from the {@code RelDataType} on each call,
 * never cached on the mapper instance.
 */
public abstract class BaseTranslatorMapper {

    /**
     * Wide entry point for bound translation. Default strips query-level date params and delegates
     * to the narrow form. Only mappers needing format/timeZone override this. Mirrors
     * {@code SimpleMappedFieldType.rangeQuery} which is final and delegates to a narrow form.
     *
     * @param r the bound request carrying value, inclusivity, format, timeZone, field, and context
     * @return a comparison RexNode for the bound
     * @throws ConversionException if the field type does not support range queries
     */
    public RexNode translateBound(BoundRequest r) throws ConversionException {
        return translateBound(r.value(), r.isLower(), r.inclusive(), r.field(), r.ctx());
    }

    /**
     * Narrow form implemented by most mappers. Default throws, mirroring legacy
     * {@code MappedFieldType.rangeQuery} at line 216 which throws IllegalArgumentException.
     *
     * @param value the processed bound value
     * @param isLower true for a lower bound
     * @param inclusive raw inclusivity from the query
     * @param field the target field definition
     * @param ctx the conversion context
     * @return a comparison RexNode for the bound
     * @throws ConversionException if the field type does not support range queries
     */
    protected RexNode translateBound(Object value, boolean isLower, boolean inclusive, RelDataTypeField field, ConversionContext ctx)
        throws ConversionException {
        throw new ConversionException("Field [" + field.getName() + "] of type [" + field.getType() + "] does not support range queries");
    }

    /**
     * Converts one value to a typed literal for term/terms queries.
     * {@code Optional.empty()} means the value can never match (e.g. a fractional unsigned_long).
     * Abstract, mirroring legacy {@code MappedFieldType.termQuery} at line 188.
     *
     * @param value the term value
     * @param field the target field definition
     * @param ctx the conversion context
     * @return a literal RexNode, or empty if the value cannot match
     * @throws ConversionException if term translation fails
     */
    public abstract Optional<RexNode> toTermLiteral(Object value, RelDataTypeField field, ConversionContext ctx) throws ConversionException;
}
