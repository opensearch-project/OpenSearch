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

package org.opensearch.cluster.block;

import org.opensearch.common.annotation.PublicApi;

import java.util.EnumSet;

/**
 * What level to block the cluster
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public enum ClusterBlockLevel {
    READ,
    WRITE,
    METADATA_READ,
    METADATA_WRITE,
    CREATE_INDEX;

    public static final EnumSet<ClusterBlockLevel> ALL = EnumSet.allOf(ClusterBlockLevel.class);
    public static final EnumSet<ClusterBlockLevel> READ_WRITE = EnumSet.of(READ, WRITE);
    public static final EnumSet<ClusterBlockLevel> WRITE_BLOCK = EnumSet.of(WRITE);
}
