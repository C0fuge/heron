// Arrow Flight SQL connector for StarRocks — FE side.
//
// Compiled ONLY with  -Pstarrocks  (needs com.starrocks:fe-core on the classpath, i.e.
// a local StarRocks source build). StarRocks has no connector plugin SPI: this class is
// wired in by adding an entry to the com.starrocks.connector.ConnectorType enum — see
// ../../../../../../patch/ConnectorType.java.patch.
//
// Scope note: this provides METADATA only (databases / tables / columns resolved from the
// Flight SQL server). Actually scanning rows requires a BE-side reader, which does not
// exist. For a working setup today use the JDBC catalog with the Arrow Flight SQL JDBC
// driver — see ../../../../../../jdbc-catalog/ and ../../../../../../DESIGN.md.

package com.starrocks.connector.arrowflight;

import com.starrocks.connector.Connector;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.exception.StarRocksConnectorException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pro.surpin.arrowflight.common.FlightConfig;
import pro.surpin.arrowflight.common.FlightSqlSession;

import java.util.Map;

public class ArrowFlightConnector
        implements Connector
{
    private static final Logger LOG = LogManager.getLogger(ArrowFlightConnector.class);

    // catalog PROPERTIES keys
    public static final String HOST = "arrow_flight.host";
    public static final String PORT = "arrow_flight.port";
    public static final String USER = "arrow_flight.user";
    public static final String PASSWORD = "arrow_flight.password";
    public static final String TOKEN = "arrow_flight.bearer_token";
    public static final String DEFAULT_SCHEMA = "arrow_flight.default_schema";
    public static final String TLS_ENABLED = "arrow_flight.tls.enabled";

    private final String catalogName;
    private final FlightSqlSession session;
    private final ArrowFlightMetadata metadata;

    // Discovered reflectively by ConnectorFactory.createRealConnector: constructor(ConnectorContext).
    public ArrowFlightConnector(ConnectorContext context)
    {
        this.catalogName = context.getCatalogName();
        Map<String, String> props = context.getProperties();

        String host = props.get(HOST);
        if (host == null || host.isEmpty()) {
            throw new StarRocksConnectorException("Missing required property '" + HOST + "'");
        }
        FlightConfig config = FlightConfig.builder(host)
                .port(Integer.parseInt(props.getOrDefault(PORT, "32010")))
                .user(props.get(USER))
                .password(props.get(PASSWORD))
                .bearerToken(props.get(TOKEN))
                .defaultSchema(props.get(DEFAULT_SCHEMA))
                .tlsEnabled(Boolean.parseBoolean(props.getOrDefault(TLS_ENABLED, "false")))
                .build();

        this.session = new FlightSqlSession(config);
        this.metadata = new ArrowFlightMetadata(catalogName, session);
        LOG.info("Created arrow_flight connector for catalog {} -> {}:{}", catalogName, host, config.getPort());
    }

    @Override
    public ConnectorMetadata getMetadata()
    {
        return metadata;
    }

    @Override
    public void shutdown()
    {
        session.close();
    }
}
