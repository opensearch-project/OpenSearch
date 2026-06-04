/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.profile.query;

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;

/**
 * Collector manager which supports profiling
 *
 * @opensearch.internal
 */
public interface ProfileCollectorManager<C extends Collector, T> extends CollectorManager<C, T>, InternalProfileComponent {}
