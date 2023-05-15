/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3.utils;

import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.auth.signer.Aws4UnsignedPayloadSigner;
import software.amazon.awssdk.auth.signer.AwsS3V4Signer;
import software.amazon.awssdk.core.signer.NoOpSigner;
import software.amazon.awssdk.core.signer.Signer;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public enum AwsRequestSigner {

    VERSION_FOUR_SIGNER("AWS4SignerType", Aws4Signer.create()),
    VERSION_FOUR_UNSIGNED_PAYLOAD_SIGNER("AWS4UnsignedPayloadSignerType", Aws4UnsignedPayloadSigner.create()),
    NO_OP_SIGNER("NoOpSignerType", new NoOpSigner()),
    S3_V4_SIGNER("AWSS3V4SignerType", AwsS3V4Signer.create());

    private final String name;
    private final Signer signer;

    AwsRequestSigner(String name, Signer signer) {
        this.name = name;
        this.signer = signer;
    }

    public String getName() {
        return name;
    }

    public Signer getSigner() {
        return signer;
    }

    public static AwsRequestSigner fromSignerName(String signerName) {
        List<AwsRequestSigner> matchingSigners = Arrays.stream(AwsRequestSigner.values())
            .filter(awsRequestSigner -> awsRequestSigner.getName().equals(signerName))
            .collect(Collectors.toList());
        if (matchingSigners.size() < 1) {
            throw new IllegalArgumentException("No matching signers found for signerName: " + signerName);
        } else if (matchingSigners.size() > 1) {
            throw new IllegalArgumentException("More than 1 matching signers found for signerName: " + signerName);
        }
        return matchingSigners.get(0);
    }
}
