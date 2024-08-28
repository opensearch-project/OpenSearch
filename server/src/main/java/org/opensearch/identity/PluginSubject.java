/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Similar to {@link Subject}, but represents a plugin executing actions
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface PluginSubject extends Subject {}
