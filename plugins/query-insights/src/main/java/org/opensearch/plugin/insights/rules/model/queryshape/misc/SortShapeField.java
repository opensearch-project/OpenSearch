/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.model.queryshape.misc;

import java.util.Objects;

public class SortShapeField {
    String fieldName;
    String sortOrder;

    @Override
    public int hashCode() {
        return Objects.hash(fieldName, sortOrder);
    }
}
