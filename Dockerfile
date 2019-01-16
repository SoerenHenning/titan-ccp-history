FROM openjdk:11-slim

ADD build/distributions/titanccp-history.tar /

EXPOSE 80

CMD export JAVA_OPTS=-Dorg.slf4j.simpleLogger.defaultLogLevel=$LOG_LEVEL \
    && /titanccp-history/bin/titanccp-history