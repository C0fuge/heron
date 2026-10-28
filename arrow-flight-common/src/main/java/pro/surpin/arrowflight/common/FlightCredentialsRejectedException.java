package pro.surpin.arrowflight.common;

/**
 * The server refused the configured credentials at the handshake (UNAUTHENTICATED). Final: asking
 * again with the same credentials gives the same answer and, against a directory with an
 * account-lockout policy, counts as one more failed login.
 */
public class FlightCredentialsRejectedException
        extends RuntimeException
{
    public FlightCredentialsRejectedException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
