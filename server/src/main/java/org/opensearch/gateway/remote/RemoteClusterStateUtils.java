/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Utility class for Remote Cluster State
 */
public class RemoteClusterStateUtils {
    public static final String PATH_DELIMITER = "/";

    public static String encodeString(String content) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(content.getBytes(StandardCharsets.UTF_8));
    }
}
