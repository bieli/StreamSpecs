#!/usr/bin/env bash
# Prefer SDKMAN JDK 17 if present, else JAVA_HOME / system java.
if [[ -d "${HOME}/.sdkman/candidates/java/17.0.8-jbr" ]]; then
  export JAVA_HOME="${HOME}/.sdkman/candidates/java/17.0.8-jbr"
elif [[ -d /usr/lib/jvm/java-17-openjdk-amd64 ]]; then
  export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
fi
export PATH="${JAVA_HOME}/bin:${PATH}"
exec "$@"
