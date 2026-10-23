#!/usr/bin/env bash
# Fails when the jars bundled in a built plugin zip differ from the committed list
# (trino-arrow-flight/bundle/trino-<version>.txt), so a dependency version can only change on purpose.
# Names are as the trino-maven-plugin writes them (groupId_artifactId-version.jar, cut to 64 characters).
# After an intended change, regenerate the list from the zip (or copy the '+' lines this script prints):
#   unzip -Z1 trino-arrow-flight/target/trino-arrow-flight-*.zip | grep '\.jar$' | xargs -n1 basename | LC_ALL=C sort > <list>
# Usage: check-bundle.sh <plugin zip> <expected list>
set -euo pipefail
export LC_ALL=C
zip=$1 expected=$2
actual=$(unzip -Z1 "$zip" | grep '\.jar$' | xargs -n1 basename | sort)
if ! diff -u "$expected" <(echo "$actual"); then
  echo "::error::bundled jars differ from $expected (- expected, + built); update the pom pins or the list"
  exit 1
fi
# one version per family, whatever the list says (jackson-annotations: 2.20+ has no patch number)
for family in 'netty-[a-z0-9-]*-4\.' 'grpc-[a-z-]*-[0-9]' 'protobuf-java' 'jackson-'; do
  versions=$(echo "$actual" | grep -E "(^|_)$family" | grep -v -e 'tcnative' -e 'jackson-annotations' \
    | sed -E 's/.*-([0-9]+(\.[0-9]+)+(\.Final)?)(-[a-z].*)?\.jar$/\1/' | sort -u)
  if [ "$(echo "$versions" | grep -c .)" -gt 1 ]; then
    echo "::error::several versions bundled for ${family}: $(echo $versions)"
    exit 1
  fi
done
echo "bundle OK: $(echo "$actual" | wc -l) jars match $expected"
