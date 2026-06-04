/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.profile.query;

import org.opensearch.common.annotation.PublicApi;

import java.util.Collection;

/**
 * Container for an agg profiling component
 *
 * @opensearch.api
 */
@PublicApi(since = "2.9.0")
public interface InternalProfileComponent {
    /**
     * @return profile component name
     */
    String getName();

    /**
     * @return the reason this profile component has been included
     */
    String getReason();

    /**
     * @return the time taken by this profile component
     */
    long getTime();

    /**
     * @return the profiling results for this profile component
     */
    CollectorResult getCollectorTree();

    /**
     * @return the children of this profile component (if any)
     */
    Collection<? extends InternalProfileComponent> children();
}
