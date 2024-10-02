/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.accesscontrol.resources;

/**
 * This interface defines the two basic access scopes for resource-access.
 * Each plugin must implement their own scopes and manage them
 * These access scopes will then be used to verify the type of access being requested.
 *
 * @opensearch.experimental
 */
public interface ResourceAccessScope {
    String READ_ONLY = "read_only";
    String READ_WRITE = "read_write";
}
