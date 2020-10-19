FROM openjdk:11-slim

ADD build/distributions/titan-ccp-history.tar /

EXPOSE 80

CMD JAVA_OPTS="$JAVA_OPTS -Dorg.slf4j.simpleLogger.defaultLogLevel=$LOG_LEVEL" \
    /titan-ccp-history/bin/titan-ccp-history
