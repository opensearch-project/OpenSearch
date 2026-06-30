/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.response.document.common;

import org.opensearch.protobufs.ObjectMap;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class DocumentFieldProtoUtilsTests extends OpenSearchTestCase {

    public void testToProtoWithEmptyList() {
        // Create an empty list of field values
        List<Object> fieldValues = Collections.emptyList();

        // Convert to Protocol Buffer
        ObjectMap.Value value = DocumentFieldProtoUtils.toProto(fieldValues);

        // Verify the conversion
        assertNotNull("Value should not be null", value);
    }

    public void testToProtoWithSimpleValues() {
        // Create a list of field values
        List<Object> fieldValues = Arrays.asList("value1", "value2");

        // Convert to Protocol Buffer
        ObjectMap.Value value = DocumentFieldProtoUtils.toProto(fieldValues);

        // Verify the conversion
        assertNotNull("Value should not be null", value);

        // Note: The current implementation might return a default value because the implementation
        // is not yet completed. This test will need to be updated once the implementation is complete.
    }

    public void testToProtoWithNullList() {
        // Convert null to Protocol Buffer
        ObjectMap.Value value = DocumentFieldProtoUtils.toProto(null);

        // Verify the conversion
        assertNotNull("Value should not be null", value);
    }
}
