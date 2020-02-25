package titan.ccp.history.api;

/**
 * Represents an error in the user's query, for example, a malformed query parameter value.
 */
public class InvalidQueryException extends RuntimeException {

  private static final long serialVersionUID = 1L; // NOPMD

  public InvalidQueryException(final String message) {
    super(message);
  }

  public InvalidQueryException(final String message, final Throwable cause) {
    super(message, cause);
  }

  public InvalidQueryException(final Throwable cause) {
    super(cause);
  }

}
