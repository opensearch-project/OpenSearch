/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

import org.opensearch.common.annotation.ExperimentalApi;

import java.security.Principal;
import java.util.concurrent.Callable;

/**
 * Similar to {@link Subject}, but represents a plugin executing actions
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface PluginSubject {

    /**
     * Get the application-wide uniquely identifying principal
     * */
    Principal getPrincipal();

    /**
     * runAs allows the caller to run a callable function as this subject
     */
    default <T> T runAs(Callable<T> callable) throws Exception {
        callable.call();
        return null;
    };
}
