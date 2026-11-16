#!/usr/bin/env bash
# Builds a patched Trino by applying trino-patches/<version>/*.patch to the Trino source tag and recompiling only the
# touched classes against the published Trino jars of the same version (Maven Central). The result is the stock
# trino-spi-<v>.jar / trino-main-<v>.jar with those classes replaced, plus a Containerfile that layers them onto the
# stock trinodb/trino:<v> image. Every other jar of the Trino server stays the stock one.
#
# Why not a full Trino build: the patch series touches 2 SPI and 7 engine source files; a full server build is
# ~30+ min of Actions time per version and produces byte-identical jars for everything else.
#
#   trino-patches/build-overlay.sh 483 [out-dir]        (needs JDK matching the Trino version: 25 for 477/483)
#
# Output (out-dir, default trino-patches/target/<v>): trino-spi-<v>.jar, trino-main-<v>.jar, Containerfile,
# PATCHES.sha256 (hash of the patch series; cache key), classes.txt (replaced classes).
set -euo pipefail

V="${1:?trino version}"
HERE="$(cd "$(dirname "$0")" && pwd)"
OUT="${2:-$HERE/target/$V}"
PATCH_DIR="$HERE/$V"
WORK="${WORK:-$(mktemp -d)}"
mkdir -p "$OUT"
OUT="$(cd "$OUT" && pwd)"
ls "$PATCH_DIR"/*.patch >/dev/null

log() { echo "[overlay $V] $*"; }

# 1. source: sparse checkout of exactly the directories the patches touch
SRC="$WORK/trino"
if [ ! -d "$SRC/.git" ]; then
    git clone -q --depth 1 --filter=blob:none --sparse --branch "$V" https://github.com/trinodb/trino.git "$SRC"
fi
DIRS=$(grep -h '^+++ b/' "$PATCH_DIR"/*.patch | sed 's|^+++ b/||' | xargs -n1 dirname | sort -u)
# shellcheck disable=SC2086
git -C "$SRC" sparse-checkout set $DIRS
git -C "$SRC" checkout -q -- . && git -C "$SRC" clean -qfd   # rerun: start from the pristine tag
log "trino $(git -C "$SRC" rev-parse HEAD)"
for p in "$PATCH_DIR"/*.patch; do
    git -C "$SRC" apply --check "$p"
    git -C "$SRC" apply "$p"
    log "applied $(basename "$p")"
done

# 2. published jars of the same version (compile classpath + jars to patch)
DEPS="$WORK/deps"
mkdir -p "$DEPS"
cat > "$DEPS/pom.xml" <<EOF
<project xmlns="http://maven.apache.org/POM/4.0.0"><modelVersion>4.0.0</modelVersion>
<groupId>local</groupId><artifactId>trino-overlay-deps</artifactId><version>1</version>
<dependencies><dependency><groupId>io.trino</groupId><artifactId>trino-main</artifactId><version>$V</version></dependency></dependencies>
</project>
EOF
(cd "$DEPS" && mvn -q -B -ntp dependency:copy-dependencies -DoutputDirectory=lib)
STOCK_SPI="$DEPS/lib/trino-spi-$V.jar"
STOCK_MAIN="$DEPS/lib/trino-main-$V.jar"

# same class file version as the stock jar
unzip -p "$STOCK_MAIN" io/trino/metadata/MetadataManager.class > "$WORK/probe.class"
MAJOR=$(od -An -j6 -N2 -tu1 "$WORK/probe.class" | awk '{print $1*256+$2}')
RELEASE=$((MAJOR - 44))
log "class file major $MAJOR -> javac --release $RELEASE"

changed() { grep -h '^+++ b/' "$PATCH_DIR"/*.patch | sed 's|^+++ b/||' | grep "^core/$1/src/main/java/.*\.java$" | sed "s|^|$SRC/|"; }

# 3. compile the touched classes: SPI first, engine against the patched SPI
rm -rf "$WORK/out-spi" "$WORK/out-main"
mkdir -p "$WORK/out-spi" "$WORK/out-main"
# shellcheck disable=SC2046
javac --release "$RELEASE" -nowarn -proc:none -encoding UTF-8 -d "$WORK/out-spi" -cp "$DEPS/lib/*" $(changed trino-spi)
# shellcheck disable=SC2046
javac --release "$RELEASE" -nowarn -proc:none -encoding UTF-8 -d "$WORK/out-main" -cp "$WORK/out-spi:$DEPS/lib/*" $(changed trino-main)

# 4. stock jars with the recompiled classes replaced
cp "$STOCK_SPI" "$OUT/trino-spi-$V.jar"
cp "$STOCK_MAIN" "$OUT/trino-main-$V.jar"
(cd "$WORK/out-spi" && find . -name '*.class' | sort) > "$OUT/classes-spi.txt"
(cd "$WORK/out-main" && find . -name '*.class' | sort) > "$OUT/classes-main.txt"
(cd "$WORK/out-spi" && jar uf "$OUT/trino-spi-$V.jar" $(cat "$OUT/classes-spi.txt"))
(cd "$WORK/out-main" && jar uf "$OUT/trino-main-$V.jar" $(cat "$OUT/classes-main.txt"))
cat "$OUT/classes-spi.txt" "$OUT/classes-main.txt" > "$OUT/classes.txt"
cat "$PATCH_DIR"/*.patch | sha256sum | cut -c1-12 > "$OUT/PATCHES.sha256"
log "replaced $(wc -l < "$OUT/classes.txt") classes; patch series $(cat "$OUT/PATCHES.sha256")"

# sanity: the new SPI method and the new rule are in the jars
unzip -l "$OUT/trino-main-$V.jar" | grep -q 'io/trino/sql/planner/iterative/rule/PushPartialAggregationIntoTableScan.class'
javap -cp "$OUT/trino-spi-$V.jar" io.trino.spi.connector.ConnectorMetadata | grep -q applyPartialAggregation
javap -cp "$OUT/trino-spi-$V.jar" io.trino.spi.connector.ConnectorTableHandle | grep -q producesPartialAggregation

cat > "$OUT/Containerfile" <<EOF
FROM trinodb/trino:$V
COPY trino-spi-$V.jar trino-main-$V.jar /usr/lib/trino/lib/
EOF
log "done: $OUT"
