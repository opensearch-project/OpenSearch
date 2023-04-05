/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.customcodecs;

import org.opensearch.index.mapper.MapperService;

/** PerFieldMappingPostingFormatCodec. {@link org.opensearch.index.codec.PerFieldMappingPostingFormatCodec} */
public class PerFieldMappingPostingFormatCodec extends Lucene95CustomCodec {

    /**
     * Creates a new instance.
     *
     * @param compressionMode The compression mode (ZSTD or ZSTDNODICT).
     * @param mapperService The mapper service.
     */
    public PerFieldMappingPostingFormatCodec(Lucene95CustomCodec.Mode compressionMode, MapperService mapperService) {
        super(compressionMode);
    }
}
