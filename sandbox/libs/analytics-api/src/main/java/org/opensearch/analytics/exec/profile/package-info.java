/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Profile data model for the analytics engine explain/profile API.
 * Contains immutable snapshot types captured after query execution:
 * {@link org.opensearch.analytics.exec.profile.QueryProfile} (query-level),
 * {@link org.opensearch.analytics.exec.profile.StageProfile} (per-stage),
 * {@link org.opensearch.analytics.exec.profile.TaskProfile} (per-task/shard).
 */
package org.opensearch.analytics.exec.profile;
