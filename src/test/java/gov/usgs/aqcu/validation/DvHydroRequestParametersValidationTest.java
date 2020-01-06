package gov.usgs.aqcu.validation;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.in;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import gov.usgs.aqcu.parameter.DvHydrographRequestParameters;
import gov.usgs.aqcu.parameter.ReportRequestParameters;

public class DvHydroRequestParametersValidationTest {

	protected static ValidatorFactory validatorFactory;
	protected static Validator validator;

	protected DvHydrographRequestParameters params;

	@BeforeClass
	public static void createValidator() {
		validatorFactory = Validation.buildDefaultValidatorFactory();
		validator = validatorFactory.getValidator();
	}

	@Before
	public void setup() {
		params = new DvHydrographRequestParameters();
		params.setPrimaryTimeseriesIdentifier("pts");
		params.setWaterYear(2018);
	}

	@AfterClass
	public static void destroyValidator() {
		validatorFactory.close();
	}

	@Test
	public void emptyRequestParameters() {
		Set<ConstraintViolation<ReportRequestParameters>> validationErrors = validator.validate(params);
		assertEquals(1, validationErrors.size());

		assertValidationResults(validationErrors,
				":Need at least one Stat-Derived Time Series Identifier selected."
		);
	}

	@Test
	public void goodParameters() {
		params.setFirstStatDerivedIdentifier("a");
		Set<ConstraintViolation<ReportRequestParameters>> validationErrors = validator.validate(params);
		assertTrue(validationErrors.isEmpty());
	}

	public void assertValidationResults(Set<ConstraintViolation<ReportRequestParameters>> actual, String expected) {
		List<String> actualStrings = actual
				.stream()
				.map(x -> String.join(":", x.getPropertyPath().toString(), x.getMessage()))
				.collect(Collectors.toList());
		assertThat(expected, is(in(actualStrings)));
	}

}
