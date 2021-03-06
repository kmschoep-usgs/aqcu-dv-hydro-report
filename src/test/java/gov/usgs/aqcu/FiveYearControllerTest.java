package gov.usgs.aqcu;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static uk.co.datumedge.hamcrest.json.SameJSONAs.sameJSONObjectAs;

import org.json.JSONObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.security.servlet.SecurityAutoConfiguration;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.util.FileCopyUtils;

import gov.usgs.aqcu.builder.ReportBuilderService;
import gov.usgs.aqcu.client.JavaToRClient;
import gov.usgs.aqcu.model.DvHydrographReport;
import gov.usgs.aqcu.parameter.FiveYearRequestParameters;

@RunWith(SpringRunner.class)
@WebMvcTest(FiveYearController.class)
@WithMockUser
@EnableAutoConfiguration(exclude = {SecurityAutoConfiguration.class})
@AutoConfigureMockMvc
public class FiveYearControllerTest {

	@Autowired
	private MockMvc mvc;

	@MockBean
	private ReportBuilderService service;

	@MockBean
	private JavaToRClient client;

	@Test
	public void getReportHappyPathTest() throws Exception {
		DvHydrographReport reportPojo = new DvHydrographReport();
		String reportHtml = "xxx";
		given(service.buildReport(any(FiveYearRequestParameters.class), anyString(), anyString())).willReturn(reportPojo);
		given(client.render(anyString(), anyString(), anyString())).willReturn(reportHtml.getBytes());

		mvc.perform(get("/fiveyeargwsum?primaryTimeseriesIdentifier=a&lastMonths=2&firstStatDerivedIdentifier=aa"))
			.andExpect(status().isOk())
			.andExpect(content().string(reportHtml))
		;

		verify(service).buildReport(any(FiveYearRequestParameters.class), anyString(), anyString());
		verify(client).render(anyString(), anyString(), anyString());
	}

	@Test
	public void getJsonHappyPathTest() throws Exception {
		DvHydrographReport reportPojo = new DvHydrographReport();
		given(service.buildReport(any(FiveYearRequestParameters.class), anyString(), anyString())).willReturn(reportPojo);

		MvcResult result = mvc.perform(get("/fiveyeargwsum/rawData?primaryTimeseriesIdentifier=a&lastMonths=2&firstStatDerivedIdentifier=aa"))
			.andExpect(status().isOk())
			.andReturn()
		;

		verify(service).buildReport(any(FiveYearRequestParameters.class), anyString(), anyString());

		String expectedJson = new String(FileCopyUtils.copyToByteArray(new ClassPathResource("testResult/skeletor.json").getInputStream()));
		assertThat(new JSONObject(result.getResponse().getContentAsString()),
				sameJSONObjectAs(new JSONObject(expectedJson)));
	}

	@Test
	public void getReportSadPathTest() throws Exception {
		DvHydrographReport reportPojo = new DvHydrographReport();
		String reportHtml = "xxx";
		given(service.buildReport(any(FiveYearRequestParameters.class), anyString(), anyString())).willReturn(reportPojo);
		given(client.render(anyString(), anyString(), anyString())).willReturn(reportHtml.getBytes());

		mvc.perform(get("/fiveyeargwsum?lastMonths=2&firstStatDerivedIdentifier=aa"))
			.andExpect(status().isBadRequest())
			.andExpect(content().string(""))
		;

		verify(service, never()).buildReport(any(FiveYearRequestParameters.class), anyString(), anyString());
		verify(client, never()).render(anyString(), anyString(), anyString());
	}

	@Test
	public void getJsonSadPathTest() throws Exception {
		DvHydrographReport reportPojo = new DvHydrographReport();
		given(service.buildReport(any(FiveYearRequestParameters.class), anyString(), anyString())).willReturn(reportPojo);

		mvc.perform(get("/fiveyeargwsum/rawData?lastMonths=2&firstStatDerivedIdentifier=aa"))
			.andExpect(status().isBadRequest())
			.andExpect(content().string(""))
		;

		verify(service, never()).buildReport(any(FiveYearRequestParameters.class), anyString(), anyString());
	}

	@Test
	public void getRequestingUserTest() {
		DVHydroController c = new DVHydroController(null, null, null);
		assertEquals("user", c.getRequestingUser());
	}

}
