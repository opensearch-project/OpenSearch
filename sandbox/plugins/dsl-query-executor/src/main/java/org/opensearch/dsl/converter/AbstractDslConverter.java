/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.converter;

import org.apache.calcite.rel.RelNode;

/**
 * Base class for DSL-to-RelNode converters.
 * Subclasses implement {@link #isApplicable} to decide whether to run,
 * and {@link #doConvert} for the actual conversion.
 */
public abstract class AbstractDslConverter {

    /** Creates a converter. */
    protected AbstractDslConverter() {}

    /**
     * Converts the input if applicable, otherwise returns it unchanged.
     *
     * @param input the current plan
     * @param ctx the conversion context
     * @return the converted or unchanged plan
     * @throws ConversionException if conversion fails
     */
    public RelNode convert(RelNode input, ConversionContext ctx) throws ConversionException {
        if (!isApplicable(ctx)) {
            return input;
        }
        return doConvert(input, ctx);
    }

    /**
     * Returns true if this converter should run for the given context.
     *
     * @param ctx the conversion context
     */
    protected abstract boolean isApplicable(ConversionContext ctx);

    /**
     * Performs the actual conversion.
     *
     * @param input the current plan
     * @param ctx the conversion context
     * @return the converted plan
     * @throws ConversionException if conversion fails
     */
    protected abstract RelNode doConvert(RelNode input, ConversionContext ctx) throws ConversionException;
}
