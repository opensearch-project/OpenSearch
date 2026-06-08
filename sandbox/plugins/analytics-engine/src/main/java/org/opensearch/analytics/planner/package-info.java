/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Coordinator-side query planner for the Analytics Plugin.
 * Converts logical Calcite RelNode trees into annotated OpenSearch RelNodes
 * with viable backend lists via RBO (HepPlanner) and CBO (VolcanoPlanner).
 */
package org.opensearch.analytics.planner;
