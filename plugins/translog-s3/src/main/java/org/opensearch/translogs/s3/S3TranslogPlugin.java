/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.translogs.s3;

import org.opensearch.plugins.Plugin;

/**
 * A plugin to add a translog type that writes to and from AWS S3.
 */
public class S3TranslogPlugin extends Plugin {

    /**
     * Dummy empty constructor
     */
    public S3TranslogPlugin() {}
}
