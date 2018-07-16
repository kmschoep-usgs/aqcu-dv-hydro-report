package gov.usgs.aqcu.config;

import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.CacheControl;
import org.springframework.http.HttpHeaders;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.GsonHttpMessageConverter;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;
import org.springframework.web.servlet.resource.WebJarsResourceResolver;
import org.springframework.security.oauth2.provider.authentication.OAuth2AuthenticationDetails;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.authentication.AnonymousAuthenticationToken;

import com.google.gson.Gson;

import gov.usgs.aqcu.serializer.SwaggerGsonSerializer;
import gov.usgs.aqcu.util.AqcuGsonBuilderFactory;
import springfox.documentation.spring.web.json.Json;

import feign.RequestInterceptor;
import feign.RequestTemplate;

@Configuration
public class WebMvcConfig extends WebMvcConfigurerAdapter {
	private static final Logger LOG = LoggerFactory.getLogger(WebMvcConfig.class);

	@Bean
	public WebMvcConfigurer corsConfigurer() {
		return new WebMvcConfigurerAdapter() {
			@Override
			public void addCorsMappings(CorsRegistry registry) {
				registry.addMapping("/**");
			}
		};
	}

	@Override
	public void addResourceHandlers(ResourceHandlerRegistry registry) {
		registry.addResourceHandler("/static/**")
				.addResourceLocations("/resources/", "/webjars/")
				.setCacheControl(
						CacheControl.maxAge(30L, TimeUnit.DAYS).cachePublic())
				.resourceChain(true)
				.addResolver(new WebJarsResourceResolver());
	}

	@Override
	public void configureMessageConverters(List<HttpMessageConverter < ? >> converters) {
		GsonHttpMessageConverter gsonHttpMessageConverter = new GsonHttpMessageConverter();
		gsonHttpMessageConverter.setGson(gson());
		converters.add(gsonHttpMessageConverter);
	}

	@Bean
	public Gson gson() {
		return AqcuGsonBuilderFactory.getConfiguredGsonBuilder()
			.registerTypeAdapter(Json.class, new SwaggerGsonSerializer())
			.serializeNulls()
			.create();
	}

	@Bean
	public RequestInterceptor requestTokenBearerInterceptor() {
		return new RequestInterceptor() {
			@Override
			public void apply(RequestTemplate requestTemplate) {
				LOG.debug("Interceptor hit..");
				ServletRequestAttributes requestAttributes = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
				if (requestAttributes == null) {
					LOG.error("Unable to get source request attributes.");
					return;
				}
				HttpServletRequest request = requestAttributes.getRequest();
				if (request == null) {
					LOG.error("Unable to get source request from attributes.");
					return;
				}

				String auth = request.getHeader(HttpHeaders.AUTHORIZATION);
				if(auth == null) {
					LOG.error("Requesting user has no authentication token.");
					return;
				}
				requestTemplate.header(HttpHeaders.AUTHORIZATION, auth);
			}
		};
	}
}
