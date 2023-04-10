/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline.common;

import org.opensearch.search.pipeline.Processor;

/**
 * Base class for common processor behavior.
 */
abstract class AbstractProcessor implements Processor {
    private final String tag;
    private final String description;

    protected AbstractProcessor(String tag, String description) {
        this.tag = tag;
        this.description = description;
    }

    @Override
    public String getTag() {
        return tag;
    }

    @Override
    public String getDescription() {
        return description;
    }
}
