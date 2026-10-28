# Auth check: Flight TLS + the server's optional login modules (server builds since a9e43ca5e296, release asset
# flight-auth-1.0.0-<build>.tar.zst: auth/ldap, auth/jwt). Sourced by run-e2e.sh; uses its variables and functions (WORK,
# OUT, NET, HOSTS, QUERIES, COMPARE, SERVER_BUILD, ASSETS, start_server, start_trino, run_query, log, status).
#
# auth_run (AUTH=only: the whole run; AUTH=1, or auto on 3 servers: at the end of the suite): the servers are restarted
# with TLS (throwaway CA, server certificate for flight1..3) and
#   ldap  FLIGHT_AUTH_PROVIDER=ldap against an UnboundID in-memory directory (container 'ldap', the SDK jar from the
#         asset), user DN template, session tokens of FLIGHT_LDAP_SESSION_MINUTES=1. Catalog flight_ldap = user/password
#         (the plugin's BasicAuth handshake, then the server's session bearer token on every call and on the other
#         members' data streams), TLS truststore as JKS. AUTH_QUERIES must be exact; after the session token expired
#         (AUTH_EXPIRY_WAIT s) one more query must be exact (re-login). Must fail: wrong password, TLS without the CA.
#   jwt   FLIGHT_AUTH_PROVIDER=jwt with a JWK set file (e2e/ci/jwt_make.py, RS256). Catalog flight_jwt = bearer token,
#         TLS truststore as PEM. AUTH_QUERIES exact; must fail: expired token, token signed by another key. If the
#         servers do not start with the jwt module the step is reported as SKIP (INFO, server side) and does not fail
#         the job.
# The module jars go first on the server classpath: the server jar bundles nimbus-jose-jwt 10.0.1, and with it the jwt
# module (built against 10.10) stops at startup with a NullPointerException in DefaultJWTClaimsVerifier (a9e43ca5e296).
# Every "must fail" query must fail (an answer is FAIL) with an authentication / TLS error. The wrong password must be
# refused without retries (ldap-wrong-password-attempts: directory access log). Result: auth-summary.tsv.
# Passwords, keys and tokens are throwaway values generated here.

AUTH_QUERIES="${AUTH_QUERIES:-q01 q03 q18}"
AUTH_EXPIRY_WAIT="${AUTH_EXPIRY_WAIT:-70}"
AUTH_DIR="$WORK/auth"
AUTH_ISSUER=https://issuer.e2e.invalid
AUTH_AUDIENCE=arrow-flight-e2e

auth_note() {  # check result detail
  printf '%s\t%s\t%s\n' "$1" "$2" "$3" | tee -a "$OUT/auth-summary.tsv"
  [ "$2" != FAIL ] || status=1
}

auth_setup() {
  local asset="$ASSETS/flight-auth-1.0.0-$SERVER_BUILD.tar.zst"
  [ -f "$asset" ] || { auth_note setup FAIL "no $(basename "$asset") in the assets"; return 1; }
  tar --zstd -xf "$asset" -C "$WORK/server"
  for m in ldap jwt; do (cd "$WORK/server/auth/$m" && sha256sum --quiet -c SHA256SUMS) || { auth_note setup FAIL "auth/$m checksums"; return 1; }; done
  mkdir -p "$AUTH_DIR/tls"
  (
    cd "$AUTH_DIR/tls"
    openssl req -x509 -newkey rsa:2048 -nodes -keyout ca.key -out ca.pem -days 2 -subj /CN=e2e-ca 2>/dev/null
    openssl req -newkey rsa:2048 -nodes -keyout server.key -out server.csr -subj /CN=flight 2>/dev/null
    printf 'subjectAltName=DNS:%s\nbasicConstraints=CA:FALSE\nkeyUsage=digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\n' \
      "${HOSTS//,/,DNS:}" > ext.cnf
    openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial -out server.pem -days 2 -extfile ext.cnf 2>/dev/null
    keytool -importcert -noprompt -alias e2e-ca -file ca.pem -keystore ca.jks -storepass changeit >/dev/null
    chmod a+r ./*
  )
  AUTH_PASSWORD="$(openssl rand -hex 16)"
  cat > "$AUTH_DIR/users.ldif" <<LDIF
dn: dc=e2e
objectClass: domain
dc: e2e

dn: ou=people,dc=e2e
objectClass: organizationalUnit
ou: people

dn: uid=e2e,ou=people,dc=e2e
objectClass: inetOrgPerson
uid: e2e
cn: e2e
sn: e2e
userPassword: $AUTH_PASSWORD
LDIF
  openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out "$AUTH_DIR/jwt.key" 2>/dev/null
  openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out "$AUTH_DIR/other.key" 2>/dev/null
  python3 "$REPO/e2e/ci/jwt_make.py" jwks "$AUTH_DIR/jwt.key" "$AUTH_DIR/jwks.json"
  chmod a+r "$AUTH_DIR"/*
}

# auth_catalog name key=value... : catalog file + mount for start_trino (host flight1 or AUTH_HOST, TLS)
auth_catalog() {
  local name=$1; shift
  { echo connector.name=arrow_flight; echo "arrow-flight.host=${AUTH_HOST:-flight1}"; echo "arrow-flight.port=${AUTH_PORT:-32010}"
    echo arrow-flight.tls.enabled=true; printf '%s\n' "$@"; } > "$WORK/etc/catalog/$name.properties"
  EXTRA_CATALOG_MOUNTS+=(-v "$WORK/etc/catalog/$name.properties:/etc/trino/catalog/$name.properties:ro")
}

# auth_servers provider env...: restart every server with TLS + the given login module; 0 = all started
auth_servers() {
  local provider=$1 h i; shift
  for h in ${HOSTS//,/ }; do docker rm -f "$h" >/dev/null 2>&1 || true; done
  SERVER_AUTH_ARGS=(-e FLIGHT_TLS_CERT=/auth/tls/server.pem -e FLIGHT_TLS_KEY=/auth/tls/server.key -e "FLIGHT_AUTH_PROVIDER=$provider")
  for i in "$@"; do SERVER_AUTH_ARGS+=(-e "$i"); done
  SERVER_CP_AUTH="/app/auth/$provider/*:"
  for h in ${HOSTS//,/ }; do start_server "$h"; done
  for h in ${HOSTS//,/ }; do
    for i in $(seq 1 90); do
      grep -q 'Server Arrow Flight SQL started' <(docker logs "$h" 2>&1) && break
      docker ps -q -f name="$h" -f status=running | grep -q . || break
      sleep 2
    done
    docker logs "$h" > "$OUT/auth/$provider-$h.log" 2>&1 || true
    grep -q 'Server Arrow Flight SQL started on Location{uri=grpc+tls' "$OUT/auth/$provider-$h.log" || return 1
  done
}

# auth_exact label catalog queries...: answers vs the reference
auth_exact() {
  local label=$1 catalog=$2 dir="$OUT/auth/$1"; shift 2
  mkdir -p "$dir"
  for q in "$@"; do QUERY_CATALOG=$catalog run_query "$q" "$dir"; done
  if "${COMPARE[@]}" "$dir" "$QUERIES" "$dir/summary.tsv" "$*" >/dev/null; then
    auth_note "$label" OK "exact: $*"
  else
    auth_note "$label" FAIL "$(tail -n +2 "$dir/summary.tsv" | grep -v OK | tr '\t\n' '  ' | cut -c1-300)"
    for q in "$@"; do [ -f "$dir/$q.err" ] && grep -m1 -E 'Query .* failed|ERROR' "$dir/$q.err" | cut -c1-300; done
  fi
}

# auth_refused label catalog pattern: a query must fail with an error matching pattern (grep -iE)
auth_refused() {
  local label=$1 catalog=$2 pattern=$3 err
  if docker exec trino trino --catalog "$catalog" --schema tpch --output-format TSV \
       --execute 'SELECT count(*) FROM nation' > "$OUT/auth/$label.tsv" 2> "$OUT/auth/$label.err"; then
    auth_note "$label" FAIL "answered '$(cat "$OUT/auth/$label.tsv")' instead of failing"
    return
  fi
  err=$(grep -m1 -E 'Query .* failed' "$OUT/auth/$label.err" | cut -c1-300)
  if grep -qiE "$pattern" "$OUT/auth/$label.err"; then
    auth_note "$label" OK "refused: $err"
  else
    auth_note "$label" FAIL "failed, but not with /$pattern/: $err"
  fi
}

# failed binds in the directory's access log (LDAP resultCode 49 = invalid credentials)
ldap_failed_binds() {
  docker logs ldap 2>&1 | grep -c 'resultCode=49' || true
}

# auth_refused_once label catalog pattern: as auth_refused, and the rejected password costs one bind per
# lookup (no retries, no extra table listing): at most 2 failed binds and a refusal within 10 s
auth_refused_once() {
  local before after start took
  before=$(ldap_failed_binds); start=$(date +%s)
  auth_refused "$@"
  took=$(( $(date +%s) - start )); after=$(ldap_failed_binds)
  if [ "$after" -eq 0 ]; then
    auth_note "$1-attempts" FAIL "no failed bind in the directory's access log (took ${took} s)"
  elif [ $((after - before)) -le 2 ] && [ "$took" -le 10 ] && ! grep -q 'failed after [0-9]* attempts' "$OUT/auth/$1.err"; then
    auth_note "$1-attempts" OK "$((after - before)) failed bind(s), refused in ${took} s"
  else
    auth_note "$1-attempts" FAIL "$((after - before)) failed binds, refused in ${took} s: $(grep -m1 -o 'failed after [0-9]* attempts' "$OUT/auth/$1.err")"
  fi
}

# auth_refused_final label catalog pattern setting: as auth_refused, and the TLS refusal is final: within 10 s,
# no "failed after N attempts", and the message names the catalog setting to fix
auth_refused_final() {
  local setting=$4 start took
  start=$(date +%s)
  auth_refused "$1" "$2" "$3"
  took=$(( $(date +%s) - start ))
  if [ "$took" -le 10 ] && ! grep -q 'failed after [0-9]* attempts' "$OUT/auth/$1.err" \
       && grep -qF "$setting" "$OUT/auth/$1.err"; then
    auth_note "$1-final" OK "refused in ${took} s, names $setting"
  else
    auth_note "$1-final" FAIL "refused in ${took} s: $(grep -m1 -oE 'failed after [0-9]* attempts|Arrow Flight server [^.]*' "$OUT/auth/$1.err")"
  fi
}

auth_run() {
  local secret good expired forged
  mkdir -p "$OUT/auth"
  printf 'check\tresult\tdetail\n' > "$OUT/auth-summary.tsv"
  auth_setup || return
  log "auth: ldap directory"
  docker run -d --name ldap --network "$NET" -v "$WORK/server":/app:ro -v "$AUTH_DIR":/auth:ro eclipse-temurin:21-jdk \
    java -cp '/app/auth/ldap/*' com.unboundid.ldap.listener.InMemoryDirectoryServerTool \
    --baseDN dc=e2e --port 389 --ldifFile /auth/users.ldif --accessLogToStandardOut >/dev/null
  for i in $(seq 1 30); do docker logs ldap 2>&1 | grep -q 'Listening for client connections' && break; sleep 1; done
  docker logs ldap 2>&1 | tail -3

  auth_catalog flight_ldap arrow-flight.user=e2e "arrow-flight.password=$AUTH_PASSWORD" \
    arrow-flight.tls.truststore-path=/etc/trino/flight-tls/ca.jks arrow-flight.tls.truststore-password=changeit
  auth_catalog flight_ldap_badpw arrow-flight.user=e2e "arrow-flight.password=wrong-$AUTH_PASSWORD" \
    arrow-flight.tls.truststore-path=/etc/trino/flight-tls/ca.jks arrow-flight.tls.truststore-password=changeit
  auth_catalog flight_ldap_noca arrow-flight.user=e2e "arrow-flight.password=$AUTH_PASSWORD"
  # TLS against a port without TLS (Trino's own plain HTTP port answers "400 Bad Request")
  AUTH_HOST=trino AUTH_PORT=8080 auth_catalog flight_tls_plain arrow-flight.bearer-token=unused
  good=$(python3 "$REPO/e2e/ci/jwt_make.py" token "$AUTH_DIR/jwt.key" "$AUTH_ISSUER" "$AUTH_AUDIENCE" e2e 3600)
  expired=$(python3 "$REPO/e2e/ci/jwt_make.py" token "$AUTH_DIR/jwt.key" "$AUTH_ISSUER" "$AUTH_AUDIENCE" e2e -600)
  forged=$(python3 "$REPO/e2e/ci/jwt_make.py" token "$AUTH_DIR/other.key" "$AUTH_ISSUER" "$AUTH_AUDIENCE" e2e 3600)
  auth_catalog flight_jwt "arrow-flight.bearer-token=$good" arrow-flight.tls.truststore-path=/etc/trino/flight-tls/ca.pem
  auth_catalog flight_jwt_expired "arrow-flight.bearer-token=$expired" arrow-flight.tls.truststore-path=/etc/trino/flight-tls/ca.pem
  auth_catalog flight_jwt_forged "arrow-flight.bearer-token=$forged" arrow-flight.tls.truststore-path=/etc/trino/flight-tls/ca.pem
  EXTRA_CATALOG_MOUNTS+=(-v "$AUTH_DIR/tls:/etc/trino/flight-tls:ro")
  start_trino 3G

  log "auth: servers with TLS + ldap"
  secret=$(openssl rand -base64 32)
  if auth_servers ldap FLIGHT_LDAP_URL=ldap://ldap:389 FLIGHT_LDAP_ALLOW_PLAINTEXT=true \
       'FLIGHT_LDAP_USER_DN_TEMPLATE=uid={0},ou=people,dc=e2e' "FLIGHT_LDAP_SESSION_SECRET=$secret" FLIGHT_LDAP_SESSION_MINUTES=1; then
    grep -h 'Flight security' "$OUT"/auth/ldap-*.log | head -1 | cut -c1-300
    auth_exact ldap flight_ldap $AUTH_QUERIES
    auth_refused_once ldap-wrong-password flight_ldap_badpw 'UNAUTHENTICATED|password'
    auth_refused_final ldap-tls-without-ca flight_ldap_noca 'certificate|PKIX|SSL|TLS|handshake' arrow-flight.tls.truststore-path
    auth_refused_final tls-plaintext-port flight_tls_plain 'TLS' arrow-flight.tls.enabled=false
    log "auth: ldap session expiry (${AUTH_EXPIRY_WAIT} s)"
    sleep "$AUTH_EXPIRY_WAIT"
    auth_exact ldap-after-expiry flight_ldap q06
  else
    auth_note ldap FAIL "servers did not start: $(grep -h -m1 -E 'Exception|Error' "$OUT"/auth/ldap-*.log | head -1 | cut -c1-300)"
  fi

  log "auth: servers with TLS + jwt"
  if auth_servers jwt FLIGHT_JWT_JWKS_FILE=/auth/jwks.json "FLIGHT_JWT_ISSUER=$AUTH_ISSUER" "FLIGHT_JWT_AUDIENCE=$AUTH_AUDIENCE"; then
    auth_exact jwt flight_jwt $AUTH_QUERIES
    auth_refused jwt-expired flight_jwt_expired 'UNAUTHENTICATED|expired'
    auth_refused jwt-forged flight_jwt_forged 'UNAUTHENTICATED|signature|invalid'
  else
    auth_note jwt SKIP "servers did not start with the jwt module (server side): $(grep -h -m1 -E 'Exception' "$OUT"/auth/jwt-*.log | head -1 | cut -c1-300)"
  fi
  docker logs ldap > "$OUT/auth/ldap-directory.log" 2>&1 || true
  cat "$OUT/auth-summary.tsv"
}
