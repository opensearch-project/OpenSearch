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

import java.util.List;

/**
 * A class binding represents a method call that stores state. Each class binding's Java class must
 * have exactly one public constructor and one public method excluding those inherited directly
 * from {@link Object}. The canonical type name parameters provided must match those of the
 * constructor and method combined. The constructor for a class binding's Java class will be called
 * when the binding method is called for the first time at which point state may be stored for the
 * arguments passed into the constructor. The method for a binding class will be called each time
 * the binding method is called and may use the previously stored state.
 */
public class AllowlistClassBinding extends WhitelistClassBinding {
    /**
     * Standard constructor. All values must be not {@code null}.
     */
    public AllowlistClassBinding(
        String origin,
        String targetJavaClassName,
        String methodName,
        String returnCanonicalTypeName,
        List<String> canonicalTypeNameParameters,
        List<Object> painlessAnnotations
    ) {
        super(origin, targetJavaClassName, methodName, returnCanonicalTypeName, canonicalTypeNameParameters, painlessAnnotations);
    }
}
