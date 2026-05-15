/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import java.util.Set;

/**
 * A backend's window-function support: the functions it can execute and the storage
 * formats those windows apply to. The planner intersects a query's required
 * {@link WindowFunction} set against {@link BackendCapabilityProvider#windowCapabilities()}.
 *
 * @opensearch.internal
 */
public record WindowCapability(Set<WindowFunction> functions, Set<String> formats) {
}
