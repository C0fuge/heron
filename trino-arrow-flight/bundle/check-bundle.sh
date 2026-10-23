#!/usr/bin/env bash
# Fails when the jars bundled in a built plugin zip differ from the committed list
# (trino-arrow-flight/bundle/trino-<version>.txt), so a dependency version can only change on purpose.
# After an intended change, regenerate the list from the zip:
#   unzip -Z1 trino-arrow-flight/target/trino-arrow-flight-*.zip | grep '\.jar$' | xargs -n1 basename | sort > <list>
# Usage: check-bundle.sh <plugin zip> <expected list>
set -euo pipefail
zip=$1 expected=$2
actual=$(unzip -Z1 "$zip" | grep '\.jar$' | xargs -n1 basename | sort)
if ! diff -u "$expected" <(echo "$actual"); then
  echo "::error::bundled jars differ from $expected (- expected, + built); update the pom pins or the list"
  exit 1
fi
# one version per family, whatever the list says
for family in 'netty-[a-z0-9-]*-4\.' 'grpc-' 'protobuf-java' 'jackson-'; do
  versions=$(echo "$actual" | grep "^$family" | grep -v 'tcnative' \
    | sed -E 's/^[a-z0-9_-]+-([0-9][^-]*(-[a-z]+)?)\.jar$/\1/; s/^[a-z0-9_-]+-([0-9][0-9.]*(\.Final)?).*/\1/' | sort -u)
  if [ "$(echo "$versions" | grep -c .)" -gt 1 ]; then
    echo "::error::several versions bundled for ${family}: $(echo $versions)"
    exit 1
  fi
done
echo "bundle OK: $(echo "$actual" | wc -l) jars match $expected"
