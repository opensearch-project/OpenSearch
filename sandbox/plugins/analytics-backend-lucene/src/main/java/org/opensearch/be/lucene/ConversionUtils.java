/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.index.query.QueryBuilder;

import java.io.IOException;
import java.util.List;

/**
 * Reusable utilities for extracting fields and values from PPL relevance function
 * RexCall structures and serializing QueryBuilders.
 *
 * <p>PPL relevance functions encode arguments as MAP_VALUE_CONSTRUCTOR pairs:
 * {@code func(MAP('field', $ref), MAP('query', literal), [MAP('param', literal)]...)}
 * Each MAP has exactly 2 operands: key at index 0, value at index 1.
 */
final class ConversionUtils {

    private ConversionUtils() {}

    /**
     * Extracts field name from a MAP_VALUE_CONSTRUCTOR operand: MAP('field', $inputRef).
     */
    static String extractFieldFromRelevanceMap(RexCall call, int operandIndex, List<FieldStorageInfo> fieldStorage) {
        RexNode operand = call.getOperands().get(operandIndex);
        if (operand instanceof RexCall mapCall) {
            RexNode value = mapCall.getOperands().get(1);
            if (value instanceof RexInputRef inputRef) {
                return FieldStorageInfo.resolve(fieldStorage, inputRef.getIndex()).getFieldName();
            }
        }
        if (operand instanceof RexInputRef inputRef) {
            return FieldStorageInfo.resolve(fieldStorage, inputRef.getIndex()).getFieldName();
        }
        throw new IllegalArgumentException("Cannot extract field name from operand " + operandIndex + ": " + operand);
    }

    /**
     * Extracts string value from a MAP_VALUE_CONSTRUCTOR operand: MAP('key', 'value').
     */
    static String extractStringFromRelevanceMap(RexCall call, int operandIndex) {
        RexNode operand = call.getOperands().get(operandIndex);
        if (operand instanceof RexCall mapCall) {
            RexNode value = mapCall.getOperands().get(1);
            if (value instanceof RexLiteral literal) {
                return literal.getValueAs(String.class);
            }
        }
        if (operand instanceof RexLiteral literal) {
            return literal.getValueAs(String.class);
        }
        throw new IllegalArgumentException("Cannot extract string from operand " + operandIndex + ": " + operand);
    }

    /**
     * Serializes a QueryBuilder into bytes using NamedWriteable protocol.
     */
    static byte[] serializeQueryBuilder(QueryBuilder queryBuilder) {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.writeNamedWriteable(queryBuilder);
            return BytesReference.toBytes(output.bytes());
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to serialize delegated query: " + queryBuilder, exception);
        }
    }
}
