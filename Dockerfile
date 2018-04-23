FROM openjdk:10-slim

ADD build/distributions/titanccp-aggregation.tar /

EXPOSE 80

CMD /titanccp-aggregation/bin/titanccp-aggregation