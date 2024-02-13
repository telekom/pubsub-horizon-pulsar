// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.pulsar;

import de.telekom.eni.pandora.horizon.mongo.config.MongoProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.actuate.autoconfigure.metrics.cache.CacheMetricsAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 * Main application class for the Pulsar application.
 *
 * This class serves as the entry point for the Pulsar application. It is annotated with
 * {@code @SpringBootApplication} to enable the Spring Boot features. Additionally, it excludes
 * the {@code CacheMetricsAutoConfiguration} to customize the caching behavior. Configuration
 * properties related to MongoDB are enabled using {@code @EnableConfigurationProperties}.
 */
@SpringBootApplication(exclude = {CacheMetricsAutoConfiguration.class})
@EnableConfigurationProperties({MongoProperties.class})
public class PulsarApplication {

	/**
	 * Main method to start the Pulsar application.
	 *
	 * @param args Command-line arguments passed to the application.
	 */
	public static void main(String[] args) {
		SpringApplication.run(PulsarApplication.class, args);
	}

}
