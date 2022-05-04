/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.action;

/**
 * Marker interface that needs to be implemented by all {@link org.opensearch.action.ActionRequest} subclasses that are composed of
 * multiple sub-requests which relate to one or more indices.  A composite request is executed by its own transport action class
 * (e.g. {@link org.opensearch.action.search.TransportMultiSearchAction}), which goes through all sub-requests and delegates their
 * execution to the appropriate transport action (e.g. {@link org.opensearch.action.search.TransportSearchAction}) for each single item.
 *
 * @opensearch.internal
 */
public interface CompositeIndicesRequest {}
