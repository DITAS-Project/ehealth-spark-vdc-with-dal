FROM java:latest
# Default to UTF-8 file.encoding
ENV LANG C.UTF-8

# Install sbt
ENV SCALA_VERSION 2.12.6

# Install Scala
RUN wget https://downloads.typesafe.com/scala/$SCALA_VERSION/scala-$SCALA_VERSION.deb
RUN dpkg -i scala-$SCALA_VERSION.deb

# Copy the package to the container
COPY target/universal/ehealth-sample-spark-vdc-1.0.tgz /app/ehealth-sample-spark-vdc-1.0.tgz

EXPOSE 9000
RUN ["tar", "xvzf", "/app/ehealth-sample-spark-vdc-1.0.tgz", "-C", "app"]

# Copy to the app path the configuration files
COPY conf/application.conf /app/ehealth-sample-spark-vdc-1.0/conf/

ENTRYPOINT ["app/ehealth-sample-spark-vdc-1.0/bin/ehealth-sample-spark-vdc",  "-Dplay.http.secret.key='wspl4r'", "-Dconfig.file=app/ehealth-sample-spark-vdc-1.0/conf/application.conf"]

