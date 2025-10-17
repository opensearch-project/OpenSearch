/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.response.exceptions;

import org.opensearch.protobufs.ObjectMap;
import org.opensearch.search.SearchParseException;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Map;

public class SearchParseExceptionProtoUtilsTests extends OpenSearchTestCase {

    public void testMetadataToProto() throws IOException {
        // Create a mock SearchParseException with specific line and column information
        int lineNumber = 25;
        int columnNumber = 15;

        // Create a mock SearchParseException that only provides the line and column numbers
        SearchParseException exception = new MockSearchParseException(lineNumber, columnNumber);

        // Convert to Protocol Buffer
        Map<String, ObjectMap.Value> metadata = SearchParseExceptionProtoUtils.metadataToProto(exception);

        // Verify the conversion
        assertTrue("Should have line field", metadata.containsKey("line"));
        assertTrue("Should have col field", metadata.containsKey("col"));

        // Verify field values
        ObjectMap.Value lineValue = metadata.get("line");
        ObjectMap.Value colValue = metadata.get("col");

        assertEquals("line should match", lineNumber, lineValue.getInt32());
        assertEquals("col should match", columnNumber, colValue.getInt32());
    }

    /**
     * A simple mock implementation of SearchParseException for testing purposes.
     * This mock only provides the line and column numbers needed for the test.
     */
    private static class MockSearchParseException extends SearchParseException {
        private final int lineNumber;
        private final int columnNumber;

        public MockSearchParseException(int lineNumber, int columnNumber) throws IOException {
            super(null, "Test search parse error", null);
            this.lineNumber = lineNumber;
            this.columnNumber = columnNumber;
        }

        @Override
        public int getLineNumber() {
            return lineNumber;
        }

        @Override
        public int getColumnNumber() {
            return columnNumber;
        }
    }
}
