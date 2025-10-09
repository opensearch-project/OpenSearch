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

package org.opensearch.grok;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.joni.Region;

import static java.util.Collections.emptyMap;

/**
 * How to extract matches.
 */
public abstract class GrokCaptureExtracter {
    /**
     * Extract {@link Map} results. This implementation of {@link GrokCaptureExtracter}
     * is mutable and should be discarded after collecting a single result.
     */
    static class MapExtracter extends GrokCaptureExtracter {
        private final Map<String, Object> result;
        private final List<GrokCaptureExtracter> fieldExtracters;

        MapExtracter(List<GrokCaptureConfig> captureConfig) {
            result = captureConfig.isEmpty() ? emptyMap() : new HashMap<>();
            fieldExtracters = new ArrayList<>(captureConfig.size());
            for (GrokCaptureConfig config : captureConfig) {
                fieldExtracters.add(config.objectExtracter(v -> {
                    String fieldName = config.name();
                    Object existing = result.get(fieldName);
                    if (existing == null) {
                        result.put(fieldName, v);
                    } else if (existing instanceof List) {
                        @SuppressWarnings("unchecked")
                        List<Object> list = (List<Object>) existing;
                        list.add(v);
                    } else {
                        List<Object> list = new ArrayList<>();
                        list.add(existing);
                        list.add(v);
                        result.put(fieldName, list);
                    }
                }));
            }
        }

        @Override
        void extract(byte[] utf8Bytes, int offset, Region region, boolean captureAllMatches) {
            for (GrokCaptureExtracter extracter : fieldExtracters) {
                extracter.extract(utf8Bytes, offset, region, captureAllMatches);
            }
        }

        Map<String, Object> result() {
            return result;
        }
    }

    abstract void extract(byte[] utf8Bytes, int offset, Region region, boolean captureAllMatches);
}
