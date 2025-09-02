/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.response.exceptions;

import org.opensearch.core.common.ParsingException;
import org.opensearch.protobufs.ObjectMap;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

import static org.opensearch.core.common.ParsingException.UNKNOWN_POSITION;

public class ParsingExceptionProtoUtilsTests extends OpenSearchTestCase {

    public void testMetadataToProtoWithPositionInfo() {
        // Create a ParsingException with line and column information
        int lineNumber = 42;
        int columnNumber = 10;
        ParsingException exception = new ParsingException(lineNumber, columnNumber, "Test parsing error", null);

        // Convert to Protocol Buffer
        Map<String, ObjectMap.Value> metadata = ParsingExceptionProtoUtils.metadataToProto(exception);

        // Verify the conversion
        assertTrue("Should have line field", metadata.containsKey("line"));
        assertTrue("Should have col field", metadata.containsKey("col"));

        // Verify field values
        ObjectMap.Value lineValue = metadata.get("line");
        ObjectMap.Value colValue = metadata.get("col");

        assertEquals("line should match", lineNumber, lineValue.getInt32());
        assertEquals("col should match", columnNumber, colValue.getInt32());
    }

    public void testMetadataToProtoWithUnknownPosition() {
        // Create a ParsingException with unknown position
        ParsingException exception = new ParsingException(UNKNOWN_POSITION, UNKNOWN_POSITION, "Test parsing error", null);

        // Convert to Protocol Buffer
        Map<String, ObjectMap.Value> metadata = ParsingExceptionProtoUtils.metadataToProto(exception);

        // Verify the conversion - should be empty since position is unknown
        assertFalse("Should have line field", metadata.containsKey("line"));
        assertFalse("Should have col field", metadata.containsKey("col"));
    }
}
