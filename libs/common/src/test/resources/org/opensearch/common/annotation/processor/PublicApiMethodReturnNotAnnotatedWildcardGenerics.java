/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.annotation.processor;

import org.opensearch.common.annotation.PublicApi;

import java.util.Collection;
import java.util.Collections;

@PublicApi(since = "1.0.0")
public class PublicApiMethodReturnNotAnnotatedWildcardGenerics {
    public Collection<?> method() {
        return Collections.emptyList();
    }
}
