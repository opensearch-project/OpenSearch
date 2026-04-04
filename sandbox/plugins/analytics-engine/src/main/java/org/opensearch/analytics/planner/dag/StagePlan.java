/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.rel.RelNode;

/**
 * A single plan alternative for a {@link Stage}. Contains a resolved fragment
 * where every operator's viableBackends and every annotation's viableBackends
 * are narrowed to exactly one backend — deterministic per-operator backend
 * assignment for fragment conversion.
 *
 * @param resolvedFragment fragment with all viableBackends narrowed to single choices
 * @opensearch.internal
 */
public record StagePlan(RelNode resolvedFragment) {}
