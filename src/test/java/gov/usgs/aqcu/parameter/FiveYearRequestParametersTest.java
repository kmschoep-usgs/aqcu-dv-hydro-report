package gov.usgs.aqcu.parameter;

import static org.junit.Assert.assertEquals;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;

import org.junit.Test;

public class FiveYearRequestParametersTest {

	@Test
	public void getStartInstantTest() {
		FiveYearRequestParameters fiveYearParams = new FiveYearRequestParameters();
		fiveYearParams.setStartDate(LocalDate.of(2018, 1, 1));
		fiveYearParams.setEndDate(LocalDate.of(2018, 2, 1));
		assertEquals(fiveYearParams.getStartInstant(ZoneOffset.UTC), Instant.parse("2013-02-01T23:59:59.999999999Z"));
		fiveYearParams.setStartDate(LocalDate.of(2017, 1, 1));
		assertEquals(fiveYearParams.getStartInstant(ZoneOffset.UTC), Instant.parse("2013-02-01T23:59:59.999999999Z"));
	}
}
