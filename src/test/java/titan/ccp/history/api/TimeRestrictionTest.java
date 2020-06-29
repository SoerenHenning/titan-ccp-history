package titan.ccp.history.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

public class TimeRestrictionTest {

  @Test
  public void testHasFrom() {
    final TimeRestriction timeRestriction = new TimeRestriction();
    timeRestriction.setFrom(10);
    assertTrue(timeRestriction.hasFrom());
  }

  @Test
  public void testHasAfter() {
    final TimeRestriction timeRestriction = new TimeRestriction();
    timeRestriction.setAfter(10);
    assertTrue(timeRestriction.hasAfter());
  }

  @Test
  public void testHasTo() {
    final TimeRestriction timeRestriction = new TimeRestriction();
    timeRestriction.setTo(10);
    assertTrue(timeRestriction.hasTo());
  }

  @Test
  public void testHasNoFrom() {
    final TimeRestriction timeRestriction = new TimeRestriction();
    timeRestriction.setTo(10);
    assertFalse(timeRestriction.hasFrom());
  }

  @Test
  public void testHasNoAfter() {
    final TimeRestriction timeRestriction = new TimeRestriction();
    timeRestriction.setTo(10);
    assertFalse(timeRestriction.hasAfter());
  }

  @Test
  public void testHasNoTo() {
    final TimeRestriction timeRestriction = new TimeRestriction();
    timeRestriction.setFrom(10);
    assertFalse(timeRestriction.hasTo());
  }

  @Test
  public void testToStringEmpty() {
    final TimeRestriction timeRestriction = new TimeRestriction();
    assertEquals("{}", timeRestriction.toString());
  }

  @Test
  public void testToStringOnlyTo() {
    final TimeRestriction timeRestriction = new TimeRestriction();
    timeRestriction.setTo(100);
    assertEquals("{to: 100}", timeRestriction.toString());
  }

  @Test
  public void testToStringAll() {
    final TimeRestriction timeRestriction = new TimeRestriction();
    timeRestriction.setFrom(-12345);
    timeRestriction.setTo(30000);
    timeRestriction.setAfter(20);
    assertEquals("{from: -12345, to: 30000, after: 20}", timeRestriction.toString());
  }

}
