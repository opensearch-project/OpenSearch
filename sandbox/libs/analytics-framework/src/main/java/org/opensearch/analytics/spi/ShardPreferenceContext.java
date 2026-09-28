/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

/**
 * Context the framework supplies to {@link BackendShardPreference#scoreFor} — fragment-shape-
 * independent inputs the backend may use to score its preference.
 *
 * <p>Surface starts minimal (just the user-facing setting) and grows as new consumers land:
 * deleted-doc count, segment count, query-cache stats, etc. Adding a field is source-
 * compatible because {@link BackendShardPreference} implementations only read what they need.
 *
 * @param preferMetadataDriver value of {@code analytics.planner.prefer_metadata_driver}.
 *
 * @opensearch.internal
 */
public record ShardPreferenceContext(boolean preferMetadataDriver) {
}
