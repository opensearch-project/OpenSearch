/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Copyright 2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except
 * in compliance with the License. A copy of the License is located at
 *
 * http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.opensearch.encryption.frame.core;

public final class Constants {

    private Constants() {}

    /** Marker for identifying the final frame. */
    public static final int ENDFRAME_SEQUENCE_NUMBER = ~0; // is 0xFFFFFFFF

    /**
     * The identifier for non-final frames in the framing content type. This value is used as part of
     * the additional authenticated data (AAD) when encryption of content in a frame.
     */
    public static final String FRAME_STRING_ID = "Crypto Frame";

    /**
     * The identifier for the final frame in the framing content type. This value is used as part of
     * the additional authenticated data (AAD) when encryption of content in a frame.
     */
    public static final String FINAL_FRAME_STRING_ID = "Crypto Final Frame";

    public static final long MAX_FRAME_NUMBER = (1L << 32) - 1;
}
