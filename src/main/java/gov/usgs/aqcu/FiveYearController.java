package gov.usgs.aqcu;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.authentication.AnonymousAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.google.gson.Gson;

import gov.usgs.aqcu.builder.ReportBuilderService;
import gov.usgs.aqcu.client.JavaToRClient;
import gov.usgs.aqcu.model.DvHydrographReport;
import gov.usgs.aqcu.parameter.FiveYearRequestParameters;

@RestController
@RequestMapping("/fiveyeargwsum")
public class FiveYearController {
	public static final String UNKNOWN_USERNAME = "unknown";
	private Gson gson;
	private ReportBuilderService reportBuilderService;
	private JavaToRClient javaToRClient;

	@Autowired
	public FiveYearController(
			ReportBuilderService reportBuilderService,
			JavaToRClient javaToRClient,
			Gson gson) {
		this.reportBuilderService = reportBuilderService;
		this.javaToRClient = javaToRClient;
		this.gson = gson;
	}

	@GetMapping(produces={MediaType.TEXT_HTML_VALUE})
	public ResponseEntity<?> getReport(@Validated FiveYearRequestParameters requestParameters) {
		String requestingUser = getRequestingUser();
		DvHydrographReport report = reportBuilderService.buildReport(requestParameters, requestingUser, "Five Year GW Summary");
		byte[] reportHtml = javaToRClient.render(requestingUser, "dvhydrograph", gson.toJson(report, DvHydrographReport.class));
		return new ResponseEntity<byte[]>(reportHtml, new HttpHeaders(), HttpStatus.OK);
	}

	@GetMapping(value="/rawData", produces={MediaType.APPLICATION_JSON_VALUE})
	public ResponseEntity<DvHydrographReport> getReportRawData(@Validated FiveYearRequestParameters requestParameters) {
		DvHydrographReport report = reportBuilderService.buildReport(requestParameters, getRequestingUser(), "Five Year GW Summary");
		return new ResponseEntity<DvHydrographReport>(report, new HttpHeaders(), HttpStatus.OK);
	}

	String getRequestingUser() {
		String username = UNKNOWN_USERNAME;
		Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
		if (null != authentication && !(authentication instanceof AnonymousAuthenticationToken)) {
			username= authentication.getName();
		}
		return username;
	}

}
