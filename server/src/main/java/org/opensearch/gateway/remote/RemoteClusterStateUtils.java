/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.core.xcontent.ToXContent;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

/**
 * Utility class for Remote Cluster State
 */
public class RemoteClusterStateUtils {

    public static final String DELIMITER = "__";
    public static final String PATH_DELIMITER = "/";
    public static final String GLOBAL_METADATA_PATH_TOKEN = "global-metadata";
    public static final String METADATA_NAME_FORMAT = "%s.dat";
    public static final String METADATA_NAME_PLAIN_FORMAT = "%s";
    public static final int GLOBAL_METADATA_CURRENT_CODEC_VERSION = 1;

    // ToXContent Params with gateway mode.
    // We are using gateway context mode to persist all custom metadata.
    public static final ToXContent.Params FORMAT_PARAMS = new ToXContent.MapParams(
        Map.of(Metadata.CONTEXT_MODE_PARAM, Metadata.CONTEXT_MODE_GATEWAY)
    );

    public static String encodeString(String content) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(content.getBytes(StandardCharsets.UTF_8));
    }
}
