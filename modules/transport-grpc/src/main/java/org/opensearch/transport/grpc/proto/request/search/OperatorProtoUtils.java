/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search;

import org.opensearch.index.query.Operator;

/**
 * Utility class for converting Operator enums between OpenSearch and Protocol Buffers formats.
 * This class provides methods to transform operator values to ensure proper query behavior
 * in search operations.
 */
public class OperatorProtoUtils {
    private OperatorProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Similar to {@link Operator#fromString(String)}
     *
     * @param op
     * @return
     */
    public static Operator fromEnum(org.opensearch.protobufs.Operator op) {
        switch (op) {
            case OPERATOR_AND:
                return Operator.AND;
            case OPERATOR_OR:
                return Operator.OR;
            default:
                throw Operator.newOperatorException(op.toString());
        }
    }
}
