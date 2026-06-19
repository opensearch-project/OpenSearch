# Minimal systemd-enabled Ubuntu image for the DEB package tests, booted with
# /sbin/init (Debian-family analog of redhat/ubi9-init). Built from AWS ECR
# Public to avoid Docker Hub unauthenticated pull-rate limits on CodeBuild.
FROM public.ecr.aws/ubuntu/ubuntu:jammy

ENV DEBIAN_FRONTEND=noninteractive
ENV container=docker

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        systemd \
        systemd-sysv \
        dbus \
        curl \
        ca-certificates \
        gnupg \
        apt-transport-https && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    # Drop units that are pointless (and slow/noisy) inside a container.
    rm -f /lib/systemd/system/systemd-update-utmp* \
          /lib/systemd/system/systemd-tmpfiles-setup* \
          /lib/systemd/system/sysinit.target.wants/systemd-firstboot.service \
          /lib/systemd/system/multi-user.target.wants/systemd-update-utmp-runlevel.service

STOPSIGNAL SIGRTMIN+3

CMD ["/sbin/init"]
