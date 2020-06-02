FROM maven:latest

COPY lib ./lib
COPY src ./src
COPY pom.xml .
COPY session.sh .
COPY config.properties ./src

RUN mvn install
CMD ["sh", "session.sh"]

