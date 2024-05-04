/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.model.queryshape.core;

import org.opensearch.plugin.insights.rules.model.queryshape.misc.SortShapeField;

import java.util.List;
import java.util.Objects;

class SortShape {
    List<SortShapeField> sortShapeFields;

    @Override
    public int hashCode() {
        return Objects.hash(sortShapeFields);
    }
}
