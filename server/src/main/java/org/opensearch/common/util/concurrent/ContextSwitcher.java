/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util.concurrent;

import org.opensearch.common.annotation.PublicApi;

/**
 * ContextSwitcher interface
 *
 * @opensearch.api
 */
@PublicApi(since = "2.17.0")
public interface ContextSwitcher {
    ThreadContext.StoredContext switchContext();
}
