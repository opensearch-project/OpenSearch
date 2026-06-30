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

package org.opensearch.search.fetch.subphase.highlight;

import org.apache.lucene.document.Field;
import org.apache.lucene.search.highlight.Encoder;
import org.apache.lucene.search.vectorhighlight.BoundaryScanner;
import org.apache.lucene.search.vectorhighlight.FieldFragList.WeightedFragInfo;
import org.opensearch.index.mapper.MappedFieldType;

/**
 * Direct Subclass of Lucene's org.apache.lucene.search.vectorhighlight.SimpleFragmentsBuilder
 * that corrects offsets for broken analysis chains.
 *
 * @opensearch.internal
 */
public class SimpleFragmentsBuilder extends org.apache.lucene.search.vectorhighlight.SimpleFragmentsBuilder {
    protected final MappedFieldType fieldType;

    public SimpleFragmentsBuilder(MappedFieldType fieldType, String[] preTags, String[] postTags, BoundaryScanner boundaryScanner) {
        super(preTags, postTags, boundaryScanner);
        this.fieldType = fieldType;
    }

    @Override
    protected String makeFragment(
        StringBuilder buffer,
        int[] index,
        Field[] values,
        WeightedFragInfo fragInfo,
        String[] preTags,
        String[] postTags,
        Encoder encoder
    ) {
        WeightedFragInfo weightedFragInfo = FragmentBuilderHelper.fixWeightedFragInfo(fieldType, values, fragInfo);
        return super.makeFragment(buffer, index, values, weightedFragInfo, preTags, postTags, encoder);
    }
}
