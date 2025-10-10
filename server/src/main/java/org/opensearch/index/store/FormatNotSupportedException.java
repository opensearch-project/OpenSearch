/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import java.util.List;

/**
 * Exception thrown when a requested data format is not supported by the CompositeStoreDirectory.
 * This typically indicates that the required format plugin is not installed or properly configured.
 */
public class FormatNotSupportedException extends IllegalArgumentException {
    
    private final String requestedFormat;
    private final List<String> availableFormats;

    public FormatNotSupportedException(String message, String requestedFormat, List<String> availableFormats) {
        super(message);
        this.requestedFormat = requestedFormat;
        this.availableFormats = availableFormats;
    }

    public String getRequestedFormat() {
        return requestedFormat;
    }

    public List<String> getAvailableFormats() {
        return availableFormats;
    }

    /**
     * Creates a detailed error message with troubleshooting information
     */
    public static FormatNotSupportedException create(String requestedFormat, List<String> availableFormats) {
        String message = String.format(
            "Data format '%s' is not supported. Available formats: %s. " +
            "Possible causes: " +
            "(1) Format plugin not installed - check that the required plugin is available in your OpenSearch installation, " +
            "(2) Plugin not properly configured - verify plugin configuration in opensearch.yml, " +
            "(3) Format not supported in this OpenSearch version - check compatibility matrix. " +
            "To resolve: Install the required format plugin, restart OpenSearch, and verify the plugin is loaded.",
            requestedFormat, availableFormats
        );
        
        return new FormatNotSupportedException(message, requestedFormat, availableFormats);
    }
}