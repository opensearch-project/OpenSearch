/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
/**
 * This package contains constructs to detect and handle rule changes between index and in memory view of indices
 * This package will facilitate detecting, add, update, delete events during current and previous {@link org.opensearch.plugin.wlm.rule.sync.RefreshBasedSyncMechanism}
 * run and then apply these events to in memory view of Rules
 */
package org.opensearch.plugin.wlm.rule.sync.detect;
