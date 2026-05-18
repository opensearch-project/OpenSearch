/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Plan-time {@link org.opensearch.analytics.spi.ScalarFunctionAdapter} implementations
 * for the DataFusion analytics backend.
 *
 * <p>Each adapter rewrites a Calcite {@link org.apache.calcite.rex.RexCall} into a form
 * the DataFusion runtime can evaluate natively after Substrait serialization.
 */
package org.opensearch.be.datafusion.planner.adapter;
