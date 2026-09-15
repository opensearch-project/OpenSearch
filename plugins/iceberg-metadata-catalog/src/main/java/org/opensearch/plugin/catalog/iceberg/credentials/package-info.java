/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * AWS credentials resolution for the Iceberg metadata catalog plugin.
 * <p>
 * Mirrors the open-source {@code repository-s3} waterfall: IRSA web-identity token
 * → IRSA assume-role → static access/secret keys → InstanceProfile fallback.
 * The resolved provider is handed to Iceberg via the {@code client.credentials-provider}
 * reflection hook using an in-process UUID-keyed registry.
 */
package org.opensearch.plugin.catalog.iceberg.credentials;
