/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search;

import org.opensearch.index.query.Operator;
import org.opensearch.test.OpenSearchTestCase;

public class OperatorProtoUtilsTests extends OpenSearchTestCase {

    public void testFromEnumWithAnd() {
        // Call the method under test with AND operator
        Operator operator = OperatorProtoUtils.fromEnum(org.opensearch.protobufs.Operator.OPERATOR_AND);

        // Verify the result
        assertNotNull("Operator should not be null", operator);
        assertEquals("Operator should be AND", Operator.AND, operator);
    }

    public void testFromEnumWithOr() {
        // Call the method under test with OR operator
        Operator operator = OperatorProtoUtils.fromEnum(org.opensearch.protobufs.Operator.OPERATOR_OR);

        // Verify the result
        assertNotNull("Operator should not be null", operator);
        assertEquals("Operator should be OR", Operator.OR, operator);
    }

    public void testFromEnumWithUnrecognized() {
        // Call the method under test with UNRECOGNIZED operator, should throw IllegalArgumentException
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> OperatorProtoUtils.fromEnum(org.opensearch.protobufs.Operator.UNRECOGNIZED)
        );

        assertTrue("Exception message should mention no operator found", exception.getMessage().contains("operator needs to be either"));
    }

    public void testFromEnumWithNull() {
        // Call the method under test with null, should throw NullPointerException
        NullPointerException exception = expectThrows(NullPointerException.class, () -> OperatorProtoUtils.fromEnum(null));
    }
}
