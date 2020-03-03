package titan.ccp.history.api;

import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class TimeRestrictionTest {

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
