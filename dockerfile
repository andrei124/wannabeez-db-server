FROM maven:latest

COPY lib ./lib
COPY src ./src
COPY pom.xml .
COPY session.sh .
COPY config.properties ./src

RUN mvn compile install assembly:single
CMD ["sh", "session.sh"]

