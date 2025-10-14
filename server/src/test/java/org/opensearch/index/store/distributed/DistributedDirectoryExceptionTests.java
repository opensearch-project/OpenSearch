/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.distributed;

import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class DistributedDirectoryExceptionTests extends OpenSearchTestCase {

    public void testExceptionWithAllParameters() {
        String message = "Test error message";
        int directoryIndex = 2;
        String operation = "testOperation";
        IOException cause = new IOException("Root cause");
        
        DistributedDirectoryException exception = new DistributedDirectoryException(
            message, directoryIndex, operation, cause
        );
        
        assertEquals("Directory index should match", directoryIndex, exception.getDirectoryIndex());
        assertEquals("Operation should match", operation, exception.getOperation());
        assertSame("Cause should match", cause, exception.getCause());
        
        String expectedMessage = "Directory 2 operation 'testOperation' failed: Test error message";
        assertEquals("Message should be formatted correctly", expectedMessage, exception.getMessage());
    }

    public void testExceptionWithoutCause() {
        String message = "Test error message";
        int directoryIndex = 1;
        String operation = "testOperation";
        
        DistributedDirectoryException exception = new DistributedDirectoryException(
            message, directoryIndex, operation
        );
        
        assertEquals("Directory index should match", directoryIndex, exception.getDirectoryIndex());
        assertEquals("Operation should match", operation, exception.getOperation());
        assertNull("Cause should be null", exception.getCause());
        
        String expectedMessage = "Directory 1 operation 'testOperation' failed: Test error message";
        assertEquals("Message should be formatted correctly", expectedMessage, exception.getMessage());
    }

    public void testExceptionMessageFormatting() {
        DistributedDirectoryException exception = new DistributedDirectoryException(
            "File not found", 0, "openInput"
        );
        
        String message = exception.getMessage();
        assertTrue("Message should contain directory index", message.contains("Directory 0"));
        assertTrue("Message should contain operation", message.contains("openInput"));
        assertTrue("Message should contain original message", message.contains("File not found"));
    }

    public void testExceptionInheritance() {
        DistributedDirectoryException exception = new DistributedDirectoryException(
            "Test", 0, "test"
        );
        
        assertTrue("Should be instance of IOException", exception instanceof IOException);
    }

    public void testExceptionWithLongMessage() {
        String longMessage = "This is a very long error message that contains lots of details about what went wrong during the operation";
        DistributedDirectoryException exception = new DistributedDirectoryException(
            longMessage, 4, "longOperation"
        );
        
        assertTrue("Message should contain the long message", exception.getMessage().contains(longMessage));
        assertEquals("Directory index should be preserved", 4, exception.getDirectoryIndex());
        assertEquals("Operation should be preserved", "longOperation", exception.getOperation());
    }

    public void testExceptionWithSpecialCharacters() {
        String messageWithSpecialChars = "Error with special chars: !@#$%^&*()";
        String operationWithSpecialChars = "operation:with:colons";
        
        DistributedDirectoryException exception = new DistributedDirectoryException(
            messageWithSpecialChars, 3, operationWithSpecialChars
        );
        
        assertTrue("Message should handle special characters", 
                  exception.getMessage().contains(messageWithSpecialChars));
        assertEquals("Operation with special chars should be preserved", 
                    operationWithSpecialChars, exception.getOperation());
    }
}