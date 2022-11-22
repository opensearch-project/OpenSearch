FROM minio/minio:RELEASE.2022-11-17T23-20-09Z

ARG bucket
ARG accessKey
ARG secretKey

RUN mkdir -p /minio/data/${bucket}
ENV MINIO_ACCESS_KEY=${accessKey}
ENV MINIO_SECRET_KEY=${secretKey}
