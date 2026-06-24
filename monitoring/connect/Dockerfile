FROM debezium/connect:2.6

ARG JMX_AGENT_VERSION=0.20.0

USER root
RUN mkdir -p /kafka/etc && \
    curl -fsSL -o /kafka/etc/jmx_prometheus_javaagent.jar \
      "https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${JMX_AGENT_VERSION}/jmx_prometheus_javaagent-${JMX_AGENT_VERSION}.jar"

COPY jmx-config.yml /kafka/etc/config.yml

ENV KAFKA_OPTS="-javaagent:/kafka/etc/jmx_prometheus_javaagent.jar=9404:/kafka/etc/config.yml"

EXPOSE 9404

USER kafka
