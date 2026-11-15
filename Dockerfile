FROM eclipse-temurin:21-jdk

# install jq net-tools curl -- needed for aws ecs metadata
RUN apt-get update && apt-get install -y jq net-tools curl

WORKDIR /app

COPY . .

RUN chmod +x gradlew

ARG APP_ENV
ENV APP_ENV=${APP_ENV}

COPY startup-script.sh /usr/local/bin/startup-script.sh
RUN chmod +x /usr/local/bin/startup-script.sh
ENTRYPOINT ["/usr/local/bin/startup-script.sh"]

RUN ./gradlew fatJar

# jar file name comes from settings.gradle
CMD ["java","-XX:MaxRAMPercentage=80", "-jar", "build/libs/mahindra-datahandler-java-all-1.0.0.jar"]
