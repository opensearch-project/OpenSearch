/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.lucene.codecs.experimental;

import org.opensearch.index.mapper.MapperService;

public class PerFieldMappingPostingFormatCustomCodec extends Lucene92CustomCodec {

    private final MapperService mapperService;

    public PerFieldMappingPostingFormatCustomCodec(Lucene92CustomCodec.Mode compressionMode, MapperService mapperService) {
        super(compressionMode);
        this.mapperService = mapperService;
    }
}
