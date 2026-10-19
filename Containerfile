# Build/run environment for the Arrow Flight connectors. Only the toolchain is baked in;
# nothing project-related is built at image time.
#
#   podman build -t arrow-flight-connectors -f Containerfile .
#   podman run --rm -v "$PWD":/app:z -w /app -v flight-m2:/root/.m2 \
#       arrow-flight-connectors mvn -B -o clean verify
#
# JDK 25: the trino-maven-plugin (which provides the 'trino-plugin' packaging) is compiled
# for Java 25, so JDK <= 24 silently fails to register the packaging.
# Maven 3.9.16: the maven:3.9.11 images mis-handle the extension on top of that.
# The Maven repo is the podman volume flight-m2, seeded once from the host ~/.m2, so the
# build runs fully in-container and never writes to the host repository.

FROM docker.io/library/eclipse-temurin:25-jdk

ARG MAVEN_VERSION=3.9.16
ADD https://dlcdn.apache.org/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz /tmp/maven.tgz
RUN tar -xzf /tmp/maven.tgz -C /opt && rm /tmp/maven.tgz \
 && ln -s /opt/apache-maven-${MAVEN_VERSION}/bin/mvn /usr/local/bin/mvn \
 && mvn -v

WORKDIR /app
CMD ["mvn", "-B", "clean", "verify"]
