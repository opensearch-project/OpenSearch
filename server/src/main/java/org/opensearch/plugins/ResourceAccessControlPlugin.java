/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

/**
 * Class to determine presence of security plugin in the cluster.
 * If yes, security plugin will be used for resource access authorization
 *
 * @opensearch.experimental
 */
public interface ResourceAccessControlPlugin extends ResourcePlugin {}
