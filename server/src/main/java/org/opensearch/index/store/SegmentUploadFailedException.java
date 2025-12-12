/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import java.io.IOException;

/**
 * Exception thrown when segment upload to remote store fails.
 * This exception provides detailed information about the upload failure
 * to help with troubleshooting and error recovery.
 */
public class SegmentUploadFailedException extends IOException {
    
    public SegmentUploadFailedException(String message) {
        super(message);
    }
    
    public SegmentUploadFailedException(String message, Throwable cause) {
        super(message, cause);
    }
    
    /**
     * Creates a SegmentUploadFailedException with format-specific information
     */
    public static SegmentUploadFailedException forFormat(String fileName, String format, Throwable cause) {
        String message = String.format(
            "Failed to upload segment file '%s' with format '%s'. " +
            "This may indicate a format-specific issue or configuration problem. " +
            "Check that the format plugin is properly installed and configured.",
            fileName, format
        );
        return new SegmentUploadFailedException(message, cause);
    }
    
    /**
     * Creates a SegmentUploadFailedException for format not supported errors
     */
    public static SegmentUploadFailedException formatNotSupported(String fileName, String format, Throwable cause) {
        String message = String.format(
            "Cannot upload segment file '%s': format '%s' is not supported. " +
            "Install the required format plugin and restart OpenSearch to enable support for this format.",
            fileName, format
        );
        return new SegmentUploadFailedException(message, cause);
    }
}