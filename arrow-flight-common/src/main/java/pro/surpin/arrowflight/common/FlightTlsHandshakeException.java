package pro.surpin.arrowflight.common;

/**
 * The TLS handshake with the server failed on its certificate (not trusted by the configured
 * truststore, expired, or not issued for the host name), or the server does not speak TLS. Final:
 * connecting again gets the same certificate and the same answer.
 */
public class FlightTlsHandshakeException
        extends RuntimeException
{
    public FlightTlsHandshakeException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
