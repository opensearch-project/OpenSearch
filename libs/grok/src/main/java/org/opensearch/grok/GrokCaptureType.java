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

import org.opensearch.grok.GrokCaptureConfig.NativeExtracterMap;

import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

import org.joni.Region;

/**
 * The type defined for the field in the pattern.
 */
enum GrokCaptureType {
    STRING {
        @Override
        <T> T nativeExtracter(int[] backRefs, NativeExtracterMap<T> map) {
            return map.forString(emit -> rawExtracter(backRefs, emit));
        }
    },
    INTEGER {
        @Override
        <T> T nativeExtracter(int[] backRefs, NativeExtracterMap<T> map) {
            return map.forInt(emit -> rawExtracter(backRefs, str -> emit.accept(Integer.parseInt(str))));
        }
    },
    LONG {
        @Override
        <T> T nativeExtracter(int[] backRefs, NativeExtracterMap<T> map) {
            return map.forLong(emit -> rawExtracter(backRefs, str -> emit.accept(Long.parseLong(str))));
        }
    },
    FLOAT {
        @Override
        <T> T nativeExtracter(int[] backRefs, NativeExtracterMap<T> map) {
            return map.forFloat(emit -> rawExtracter(backRefs, str -> emit.accept(Float.parseFloat(str))));
        }
    },
    DOUBLE {
        @Override
        <T> T nativeExtracter(int[] backRefs, NativeExtracterMap<T> map) {
            return map.forDouble(emit -> rawExtracter(backRefs, str -> emit.accept(Double.parseDouble(str))));
        }
    },
    BOOLEAN {
        @Override
        <T> T nativeExtracter(int[] backRefs, NativeExtracterMap<T> map) {
            return map.forBoolean(emit -> rawExtracter(backRefs, str -> emit.accept(Boolean.parseBoolean(str))));
        }
    };

    abstract <T> T nativeExtracter(int[] backRefs, NativeExtracterMap<T> map);

    static GrokCaptureType fromString(String str) {
        switch (str) {
            case "string":
                return STRING;
            case "int":
                return INTEGER;
            case "long":
                return LONG;
            case "float":
                return FLOAT;
            case "double":
                return DOUBLE;
            case "boolean":
                return BOOLEAN;
            default:
                return STRING;
        }
    }

    protected final GrokCaptureExtracter rawExtracter(int[] backRefs, Consumer<? super String> emit) {
        return new GrokCaptureExtracter() {
            @Override
            void extract(byte[] utf8Bytes, int offset, Region region, boolean captureAllMatches) {
                for (int number : backRefs) {
                    if (region.getBeg(number) >= 0) {
                        int matchOffset = offset + region.getBeg(number);
                        int matchLength = region.getEnd(number) - region.getBeg(number);
                        emit.accept(new String(utf8Bytes, matchOffset, matchLength, StandardCharsets.UTF_8));
                        // return the first match value if captureAllMatches is false, else continue to capture all values
                        if (!captureAllMatches) {
                            return;
                        }
                    }
                }
            }
        };
    }
}
