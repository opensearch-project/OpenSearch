FROM ubuntu:22.04
RUN apt-get update -y
RUN apt-get install -y openjdk-21-jre-headless
ENTRYPOINT exec java -classpath "/fixture/shared/*" fixture.azure.AzureHttpFixture 0.0.0.0 8091 container
EXPOSE 8091
