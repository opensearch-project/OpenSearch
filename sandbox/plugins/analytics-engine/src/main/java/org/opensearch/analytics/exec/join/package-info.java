/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * MPP join strategy selection and broadcast-join orchestration. Houses the
 * post-CBO advisor that classifies joins as coordinator-centric, broadcast,
 * or hash-shuffle, plus the two-pass dispatcher and DAG rewriter that drive
 * the M1 broadcast path.
 */
package org.opensearch.analytics.exec.join;
