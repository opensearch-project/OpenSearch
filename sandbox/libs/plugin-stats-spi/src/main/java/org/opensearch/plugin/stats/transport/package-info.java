/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Transport and REST action base classes for per-format statistics endpoints.
 *
 * <p>Data-format plugins subclass the abstract bases in this package to register
 * their own index-level and node-level stats actions without duplicating routing,
 * serialization, or aggregation logic.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
package org.opensearch.plugin.stats.transport;

import org.opensearch.common.annotation.ExperimentalApi;
