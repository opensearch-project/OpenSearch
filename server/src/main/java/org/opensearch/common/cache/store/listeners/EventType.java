/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.store.listeners;

/**
 * Describes various event types which is used to notify the called via listener.
 *
 * @opensearch.internal
 */
public enum EventType {

    ON_MISS,
    ON_HIT,
    ON_CACHED,
    ON_REMOVAL;
}
