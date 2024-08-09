/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.action.IndicesRequest;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.querygroup.LabelingService.LabelingImplementationType;

/**
 * This plugin introduces contracts on how should the incoming requests be labeled using implicit/explicit request attributes
 *
 */
public interface LabelingPlugin {

    LabelingImplementationType getImplementationName();

    /**
     * This method will compute  label value/values and
     * put these in the {@link ThreadContext} against {@link org.opensearch.querygroup.LabelingHeader}
     * @param request
     * @param threadContext
     */
    void labelRequest(final IndicesRequest request, final ThreadContext threadContext);
}
