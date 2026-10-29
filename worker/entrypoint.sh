#!/bin/sh
set -eu
export JAVA_HOME="$(dirname "$(dirname "$(readlink -f /usr/bin/java)")")"
exec "$@"
