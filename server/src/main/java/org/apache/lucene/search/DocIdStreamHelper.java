/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.lucene.search;

import org.apache.lucene.util.FixedBitSet;

/*
Need this helper class for initializing BitSetDocIdStream as it is
package-private class in Lucene
 */
public class DocIdStreamHelper {
    public static DocIdStream getDocIdStream(FixedBitSet fixedBitSet) {
        return new BitSetDocIdStream(fixedBitSet, 0);
    }
}
