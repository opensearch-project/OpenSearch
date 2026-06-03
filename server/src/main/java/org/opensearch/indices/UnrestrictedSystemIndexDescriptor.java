/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * A {@link SystemIndexDescriptor} that has unrestricted read access (search/get) while blocking writes.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class UnrestrictedSystemIndexDescriptor extends SystemIndexDescriptor {

    /**
     * @param indexPattern The pattern of index names that this descriptor will be used for. Must start with a '.' character.
     * @param description The name of the plugin responsible for this system index.
     */
    public UnrestrictedSystemIndexDescriptor(String indexPattern, String description) {
        super(indexPattern, description);
    }

    @Override
    public String toString() {
        return "UnrestrictedSystemIndexDescriptor[pattern=[" + getIndexPattern() + "], description=[" + getDescription() + "]]";
    }
}
