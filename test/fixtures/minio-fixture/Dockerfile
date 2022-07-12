FROM minio/minio:RELEASE.2022-06-25T15-50-16Z

ARG bucket
ARG accessKey
ARG secretKey

RUN mkdir -p /minio/data/${bucket}
ENV MINIO_ROOT_USER=${accessKey}
ENV MINIO_ROOT_PASSWORD=${secretKey}
