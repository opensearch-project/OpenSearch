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

package org.opensearch.painless.spi;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * An instance binding represents a method call that stores state. Each instance binding must provide
 * exactly one public method name. The canonical type name parameters provided must match those of the
 * method. The method for an instance binding will target the specified Java instance.
 */
public class AllowlistInstanceBinding extends WhitelistInstanceBinding {
    /**
     * Standard constructor. All values must be not {@code null}.
     */
    public AllowlistInstanceBinding(String origin, Object targetInstance, String methodName, String returnCanonicalTypeName, List<String> canonicalTypeNameParameters, List<Object> painlessAnnotations) {
        super(origin, targetInstance, methodName, returnCanonicalTypeName, canonicalTypeNameParameters, painlessAnnotations);
    }
}
