/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.sandbox;

import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;

/**
 * This interface defines the key APIs for implementing Sandbox persistence
 */
public interface Perisist {
    /**
     * persists the @link Sandbox in a durable storage
     * @param sandbox
     */
    <U extends ActionResponse> void persist(Sandbox sandbox, ActionListener<U> listener);
}
