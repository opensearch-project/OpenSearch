/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3;

import org.opensearch.common.io.PathUtils;

import java.nio.file.Path;

/**
 * The trait that adds the config path to the test cases
 */
interface ConfigPathSupport {
    default Path configPath() {
        return PathUtils.get("config");
    }
}
