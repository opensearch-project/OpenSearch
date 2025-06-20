/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.identity;

import org.opensearch.common.CheckedRunnable;
import org.opensearch.common.annotation.ExperimentalApi;

import java.security.Principal;

/**
 * An individual, process, or device that causes information to flow among objects or change to the system state.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface Subject {

    /**
     * Get the application-wide uniquely identifying principal
     * */
    Principal getPrincipal();

    /**
     * runAs allows the caller to run a {@link CheckedRunnable} as this subject
     */
    default <E extends Exception> void runAs(CheckedRunnable<E> r) throws E {
        r.run();
    };
}
