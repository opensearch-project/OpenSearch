FROM ubuntu:14.04
RUN apt update -y
RUN apt upgrade -y
ADD . /fixture
RUN echo kerberos.build.opensearch.org > /etc/hostname && echo "127.0.0.1 kerberos.build.opensearch.org" >> /etc/hosts
RUN bash /fixture/src/main/resources/provision/installkdc.sh

EXPOSE 88
EXPOSE 88/udp

CMD sleep infinity
