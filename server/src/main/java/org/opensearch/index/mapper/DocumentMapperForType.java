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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.index.mapper;

import org.opensearch.common.annotation.DeprecatedApi;

/**
 * Document mapper used for types
 *
 * @opensearch.internal
 *
 * @deprecated types are being removed
 */
@Deprecated
@DeprecatedApi(since = "2.0.0")
public class DocumentMapperForType {
    private final DocumentMapper documentMapper;
    private final Mapping mapping;

    public DocumentMapperForType(DocumentMapper documentMapper, Mapping mapping) {
        this.mapping = mapping;
        this.documentMapper = documentMapper;
    }

    public DocumentMapper getDocumentMapper() {
        return documentMapper;
    }

    public Mapping getMapping() {
        return mapping;
    }
}
