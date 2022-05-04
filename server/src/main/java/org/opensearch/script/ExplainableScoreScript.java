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

package org.opensearch.script;

import org.apache.lucene.search.Explanation;
import org.opensearch.common.Nullable;

import java.io.IOException;

/**
 * To be implemented by {@link ScoreScript} which can provided an {@link Explanation} of the score
 * This is currently not used inside opensearch but it is used, see for example here:
 * https://github.com/elastic/elasticsearch/issues/8561
 *
 * @opensearch.internal
 */
public interface ExplainableScoreScript {

    /**
     * Build the explanation of the current document being scored
     * The script score needs the Explanation of the sub query score because it might use _score and
     * want to explain how that was computed.
     *
     * @param subQueryScore the Explanation for _score
     * @deprecated please use {@code explain(Explanation subQueryScore, @Nullable String scriptName)}
     */
    @Deprecated
    Explanation explain(Explanation subQueryScore) throws IOException;

    /**
     * Build the explanation of the current document being scored
     * The script score needs the Explanation of the sub query score because it might use _score and
     * want to explain how that was computed.
     *
     * @param subQueryScore the Explanation for _score
     * @param scriptName the script name
     */
    default Explanation explain(Explanation subQueryScore, @Nullable String scriptName) throws IOException {
        return explain(subQueryScore);
    }

}
