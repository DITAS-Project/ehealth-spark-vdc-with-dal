FROM java:latest
# Default to UTF-8 file.encoding
ENV LANG C.UTF-8

# Install sbt
ENV SCALA_VERSION 2.12.6

# Install Scala
RUN wget https://downloads.typesafe.com/scala/$SCALA_VERSION/scala-$SCALA_VERSION.deb
RUN dpkg -i scala-$SCALA_VERSION.deb

# Copy the package to the container
COPY target/universal/ehealth-dal-0.1.tgz /app/ehealth-dal-0.1.tgz

EXPOSE 50055
RUN ["tar", "xvzf", "/app/ehealth-dal-0.1.tgz", "-C", "app"]

# Copy to the app path the configuration files
COPY conf/ehealthGrpcConfig.yml /app/ehealth-dal-0.1/conf/

ENTRYPOINT ["app/ehealth-dal-0.1/bin/ehealth-server",  "app/ehealth-dal-0.1/conf/ehealthGrpcConfig.yml"]

