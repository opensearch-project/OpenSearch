/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Package for converting protobuf function score containers to OpenSearch ScoreFunctionBuilder instances.
 *
 * This package contains converters for various function score types including:
 * <ul>
 *   <li>Exponential decay functions</li>
 *   <li>Gaussian decay functions</li>
 *   <li>Linear decay functions</li>
 *   <li>Script score functions</li>
 *   <li>Field value factor functions</li>
 *   <li>Random score functions</li>
 * </ul>
 *
 * Each converter is responsible for converting protobuf representations to their corresponding
 * OpenSearch ScoreFunctionBuilder implementations.
 */
package org.opensearch.transport.grpc.proto.request.search.query.functionscore;
