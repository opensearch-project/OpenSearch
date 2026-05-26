/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices;

import org.opensearch.common.annotation.PublicApi;

/**
 * A {@link SystemIndexDescriptor} that allows read access (search/get) while blocking writes.
 *
 * @opensearch.api
 */
@PublicApi(since = "3.8.0")
public class ReadableSystemIndexDescriptor extends SystemIndexDescriptor {

    /**
     * @param indexPattern The pattern of index names that this descriptor will be used for. Must start with a '.' character.
     * @param description The name of the plugin responsible for this system index.
     */
    public ReadableSystemIndexDescriptor(String indexPattern, String description) {
        super(indexPattern, description);
    }

    @Override
    public String toString() {
        return "ReadableSystemIndexDescriptor[pattern=[" + getIndexPattern() + "], description=[" + getDescription() + "]]";
    }
}
