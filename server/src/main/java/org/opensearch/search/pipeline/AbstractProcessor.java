/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline;

/**
 * Base class for common processor behavior.
 */
public abstract class AbstractProcessor implements Processor {
    private final String tag;
    private final String description;
    private final boolean ignoreFailure;

    protected AbstractProcessor(String tag, String description, boolean ignoreFailure) {
        this.tag = tag;
        this.description = description;
        this.ignoreFailure = ignoreFailure;
    }

    @Override
    public String getTag() {
        return tag;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public boolean isIgnoreFailure() {
        return ignoreFailure;
    }
}
