package gov.usgs.aqcu.config;

import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.security.oauth2.provider.authentication.OAuth2AuthenticationDetails;
import org.springframework.security.core.Authentication;
import org.springframework.security.authentication.AnonymousAuthenticationToken;
import org.springframework.http.HttpHeaders;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import feign.RequestInterceptor;
import feign.RequestTemplate;
import gov.usgs.aqcu.builder.ReportBuilderService;

@Component
public class WaterAuthRequestInterceptor implements RequestInterceptor {
	private static final Logger LOG = LoggerFactory.getLogger(ReportBuilderService.class);

	@Override
    public void apply(RequestTemplate requestTemplate) {
        LOG.error("Interceptor hit..");
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        if (null != authentication && !(authentication instanceof AnonymousAuthenticationToken)) {
            OAuth2AuthenticationDetails details = (OAuth2AuthenticationDetails) authentication.getDetails();
            LOG.error("User authenticated.. Token: " + details.getTokenValue());
            requestTemplate.header(HttpHeaders.AUTHORIZATION, "Bearer " + details.getTokenValue());
        } else {
            LOG.error("Requesting user not authenticated.");
        }
    }
}
