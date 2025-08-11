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

package org.opensearch.cluster.metadata.core;

import org.opensearch.cluster.Diff;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.opensearch.cluster.metadata.MetadataUtils.SINGLE_MAPPING_NAME;

/**
 * Mapping configuration for a type.
 *
 * @opensearch.api
 */
@PublicApi(since = "3.2.0")
public class MappingMetadata extends AbstractMappingMetadata {
    public static final MappingMetadata EMPTY_MAPPINGS = new MappingMetadata(SINGLE_MAPPING_NAME, Collections.emptyMap());

    public MappingMetadata(String type, CompressedXContent mappingSource, Boolean routingRequired) {
        super(type, mappingSource, routingRequired);
    }

    public MappingMetadata(CompressedXContent mapping) {
        super(mapping);
    }

    public MappingMetadata(String type, Map<String, Object> mapping) {
        super(type, mapping);
    }

    public MappingMetadata(StreamInput in) throws IOException {
        super(in);
    }

    public static Diff<MappingMetadata> readDiffFrom(StreamInput in) throws IOException {
        return new MappingMetadataDiff(in);
    }

    /**
     * A diff of mapping metadata.
     *
     * @opensearch.internal
     */
    static class MappingMetadataDiff implements Diff<MappingMetadata> {

        private final Diff<AbstractMappingMetadata> delegate;

        MappingMetadataDiff(StreamInput in) throws IOException {
            this.delegate = AbstractMappingMetadata.readDiff(in);
        }

        @Override
        public MappingMetadata apply(MappingMetadata part) {
            AbstractMappingMetadata abstractMappingMetadata = delegate.apply(part);
            return new MappingMetadata(
                abstractMappingMetadata.type(),
                abstractMappingMetadata.source(),
                abstractMappingMetadata.routingRequired()
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            delegate.writeTo(out);
        }
    }
}
