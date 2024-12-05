/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.annotation.processor;

import org.opensearch.common.annotation.InternalApi;
import org.opensearch.common.annotation.PublicApi;

@PublicApi(since = "1.0.0")
public class PublicApiConstructorAnnotatedInternalApi {
    /**
     * The constructors have relaxed semantics at the moment: those could be not annotated or be annotated as {@link InternalApi}
     */
    @InternalApi
    public PublicApiConstructorAnnotatedInternalApi(NotAnnotated arg) {}
}
