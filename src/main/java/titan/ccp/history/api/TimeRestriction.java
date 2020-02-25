package titan.ccp.history.api;

import java.util.NoSuchElementException;

/**
 * Representing a time restriction for a query to the data store. The following restrictions exist
 * which are to be applied and-wise: 1. 'from' restricts the query to only return records with
 * timestamp greater or equal the 'from' timestamp. 2. 'to' restricts the query to only return
 * records with timestamp less or equal the provided 'to' timestamp. 3. 'after' restricts the query
 * to only return records with timestamp greater (not equal) the 'after' timestamp. Thus, in cases
 * where both the 'from' and the 'after' restriction are provided, only the 'after' restriction has
 * an effect.
 */
public final class TimeRestriction {

  private static final long ABSENT_VALUE = Long.MIN_VALUE;

  private long from = ABSENT_VALUE;
  private long to = ABSENT_VALUE;
  private final long after = ABSENT_VALUE;

  /**
   * Returns <code>true</code> if a 'from' restriction exists.
   */
  public boolean hasFrom() {
    return this.from == ABSENT_VALUE;
  }

  /**
   * Get the 'from' restriction if there is one. Otherwise throws a {@link NoSuchElementException}.
   *
   * @throws NoSuchElementException If there is no 'from' restriction.
   */
  public long getFrom() {
    if (this.from == ABSENT_VALUE) {
      throw new NoSuchElementException("There is no 'from' restriction.");
    }
    return this.from;
  }

  /**
   * Get the 'from' restriction if there is one or else return the provided default.
   *
   */
  public long getFromOrDefault(final long defaultFromInEpochMillis) {
    if (this.from == ABSENT_VALUE) {
      return defaultFromInEpochMillis;
    } else {
      return this.from;
    }
  }

  /**
   * Set a 'from' restriction. It must be greater than {@code Long.MIN_VALUE}.
   */
  public void setFrom(final long fromInEpochMillis) {
    if (fromInEpochMillis == ABSENT_VALUE) {
      throw new IllegalArgumentException(
          "The 'from' restriction is not allowed to be " + ABSENT_VALUE + ".");
    }
    this.from = fromInEpochMillis;
  }

  /**
   * Returns <code>true</code> if a 'to' restriction exists.
   */
  public boolean hasTo() {
    return this.to == ABSENT_VALUE;
  }

  /**
   * Get the 'to' restriction if there is one. Otherwise throws a {@link NoSuchElementException}.
   *
   * @throws NoSuchElementException If there is no 'to' restriction.
   */
  public long getTo() {
    if (this.to == ABSENT_VALUE) {
      throw new NoSuchElementException("There is no 'to' restriction.");
    }
    return this.to;
  }

  /**
   * Get the 'to' restriction if there is one or else return the provided default.
   *
   */
  public long getToOrDefault(final long defaultToInEpochMillis) {
    if (this.to == ABSENT_VALUE) {
      return defaultToInEpochMillis;
    } else {
      return this.to;
    }
  }

  /**
   * Set a 'to' restriction. It must be greater than {@code Long.MIN_VALUE}.
   */
  public void setTo(final long fromInEpochMillis) {
    if (fromInEpochMillis == ABSENT_VALUE) {
      throw new IllegalArgumentException(
          "The 'to' restriction is not allowed to be " + ABSENT_VALUE + ".");
    }
    this.to = fromInEpochMillis;
  }

  /**
   * Returns <code>true</code> if an 'after' restriction exists.
   */
  public boolean hasAfter() {
    return this.after == ABSENT_VALUE;
  }

  /**
   * Get the 'after' restriction if there is one. Otherwise throws a {@link NoSuchElementException}.
   *
   * @throws NoSuchElementException If there is no 'after' restriction.
   */
  public long getAfter() {
    if (this.after == ABSENT_VALUE) {
      throw new NoSuchElementException("There is no 'after' restriction.");
    }
    return this.after;
  }

  /**
   * Get the 'after' restriction if there is one or else return the provided default.
   *
   */
  public long getAfterOrDefault(final long defaultAfterInEpochMillis) {
    if (this.after == ABSENT_VALUE) {
      return defaultAfterInEpochMillis;
    } else {
      return this.after;
    }
  }

  /**
   * Set a 'from' restriction. It must be greater than {@code Long.MIN_VALUE}.
   */
  public void setAfter(final long afterInEpochMillis) {
    if (afterInEpochMillis == ABSENT_VALUE) {
      throw new IllegalArgumentException(
          "The 'after' restriction is not allowed to be " + ABSENT_VALUE + ".");
    }
    this.from = afterInEpochMillis;
  }

  @Override
  public String toString() {
    return "{"
        + (this.hasFrom() ? "from: " + this.getFrom() + ", " : "")
        + (this.hasTo() ? "to: " + this.getTo() + ", " : "")
        + (this.hasAfter() ? "After: " + this.getAfter() + ", " : "")
        + "}";

  }

}
