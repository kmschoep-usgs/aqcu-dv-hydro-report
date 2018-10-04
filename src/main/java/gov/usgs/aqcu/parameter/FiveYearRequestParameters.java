package gov.usgs.aqcu.parameter;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;

import gov.usgs.aqcu.validation.StatDerivedIdentifierPresent;

@StatDerivedIdentifierPresent
public class FiveYearRequestParameters extends DvHydrographRequestParameters {

	@Override
	public Instant getStartInstant(ZoneOffset zoneOffset) {
		if (requestPeriod == null) {
			determineRequestPeriod();
		}
		return requestPeriod.getRight().minus(5, ChronoUnit.YEARS).atTime(23,59,59,999999999).toInstant(zoneOffset);
	}
}
