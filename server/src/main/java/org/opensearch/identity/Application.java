/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.identity;

import java.security.Principal;

/**
 * A service that transmits data to and/or from OpenSearch
 *
 * @opensearch.experimental
 */
public interface Application {

    public Principal getPrincipal();
}
