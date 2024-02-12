package de.telekom.horizon.pulsar.config.rest;

import io.micrometer.observation.ObservationPredicate;
import io.micrometer.observation.ObservationRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.autoconfigure.observation.ObservationRegistryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.oauth2.server.resource.authentication.JwtIssuerAuthenticationManagerResolver;
import org.springframework.security.web.SecurityFilterChain;

import java.util.List;

import static org.springframework.boot.actuate.autoconfigure.security.servlet.EndpointRequest.toAnyEndpoint;

/**
 * Configuration class for customizing Spring Security settings in the web application.
 *
 * This class configures security settings such as OAuth2 authentication, issuer URLs, and
 * permissions for different endpoints based on the application's requirements.
 */
@EnableWebSecurity
@EnableMethodSecurity
@Slf4j
@Configuration
public class WebSecurityConfig {

    @Value("${pulsar.security.oauth:true}")
    private boolean enableOauth;

    @Value("#{'${pulsar.security.issuerUrls}'.split(',')}")
    private List<String> issuerUrls;

    /**
     * Configures the security filter chain for HTTP requests.
     *
     * @param http The {@link HttpSecurity} object to configure.
     * @return The configured {@link SecurityFilterChain}.
     * @throws Exception If an error occurs during configuration.
     */
    @Bean
    protected SecurityFilterChain gatesSecurityFilterChain(HttpSecurity http) throws Exception {
        log.info("configure security {}", (enableOauth ? "ON" : "OFF") );

        // Disable CSRF protection
        http.csrf(AbstractHttpConfigurer::disable);

        if (enableOauth) {
            // Configure OAuth2 authentication with JWT issuer resolver
            var jwtIssuerAuthenticationManagerResolver = JwtIssuerAuthenticationManagerResolver.fromTrustedIssuers(issuerUrls);

            http.authorizeHttpRequests(authorizeRequests -> authorizeRequests
                            .requestMatchers(toAnyEndpoint()).permitAll()
                            .requestMatchers(HttpMethod.HEAD, "/v1/**").permitAll()
                            .requestMatchers( "/v1/**").authenticated())
                    .oauth2ResourceServer(oauth2 -> oauth2.authenticationManagerResolver(jwtIssuerAuthenticationManagerResolver));
        } else {
            // Allow all requests without authentication if OAuth2 is disabled
            http.authorizeHttpRequests(authorizeRequests -> authorizeRequests.anyRequest().permitAll());
        }
        return http.build();
    }

    @Bean
    ObservationRegistryCustomizer<ObservationRegistry> noSpringSecurityObservations() {
        ObservationPredicate predicate = (name, context) -> !name.startsWith("spring.security.");
        return (registry) -> registry.observationConfig().observationPredicate(predicate);
    }
}
