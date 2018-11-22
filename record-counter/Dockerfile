FROM openjdk:10-slim

ADD build/distributions/record-counter.tar /

CMD export JAVA_OPTS=-Dorg.slf4j.simpleLogger.defaultLogLevel=$LOG_LEVEL \
    && /record-counter/bin/record-counter
