package gov.usgs.aqcu.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.authentication.AnonymousAuthenticationToken;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.config.annotation.web.configuration.EnableResourceServer;
import org.springframework.security.oauth2.config.annotation.web.configuration.ResourceServerConfigurerAdapter;
import org.springframework.security.oauth2.config.annotation.web.configurers.ResourceServerSecurityConfigurer;
import org.springframework.security.oauth2.provider.authentication.OAuth2AuthenticationDetails;

import feign.RequestInterceptor;
import feign.RequestTemplate;

@Configuration
@EnableResourceServer
public class OAuth2ResourceServerConfig extends ResourceServerConfigurerAdapter {

	@Value("${security.oauth2.resource.id}")
	private String resourceId;

	@Override
	public void configure(HttpSecurity http) throws Exception {
		http
			.authorizeRequests()
				.antMatchers("/", "/swagger-ui.html", "/swagger-resources/**", "/webjars/**", "/v2/**").permitAll()
				.antMatchers("/health/**", "/info/**").permitAll()
				.anyRequest().fullyAuthenticated()
			.and()
				.sessionManagement().sessionCreationPolicy(SessionCreationPolicy.STATELESS)
			.and()
				.cors()
			.and()
				.csrf().disable()
		;
	}

	@Override
	public void configure(ResourceServerSecurityConfigurer resources) {
		resources.resourceId(resourceId);
	}

	@Bean
    public RequestInterceptor requestTokenBearerInterceptor() {
        return new RequestInterceptor() {
            @Override
            public void apply(RequestTemplate requestTemplate) {
				Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
				if (null != authentication && !(authentication instanceof AnonymousAuthenticationToken)) {
					OAuth2AuthenticationDetails details = (OAuth2AuthenticationDetails) authentication.getDetails();
                	requestTemplate.header("Authorization", "bearer " + details.getTokenValue());
				}                
            }
        };
    }
}
