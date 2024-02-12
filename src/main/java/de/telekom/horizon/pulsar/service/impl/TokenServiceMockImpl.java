package de.telekom.horizon.pulsar.service.impl;

import de.telekom.horizon.pulsar.service.TokenService;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

/**
 * Mock implementation of the {@link TokenService} interface for the "publisher-mock" profile.
 *
 * This class, annotated with {@code @Service} and {@code @Profile("publisher-mock")},
 * provides a mock implementation of the {@link TokenService} interface specifically for the
 * "publisher-mock" profile. The sole method, {@code getSubscriberId()}, returns a predefined
 * mocked publisherId, {@code MOCKED_PUBLISHER_ID}.
 */
@Profile("publisher-mock")
@Service
public class TokenServiceMockImpl implements TokenService {

    public static final String MOCKED_PUBLISHER_ID = "eni--pandora--foobar";

    /**
     * Returns the predefined mocked publisherId.
     *
     * @return The mocked publisherId.
     */
    @Override
    public String getSubscriberId() {
        return MOCKED_PUBLISHER_ID;
    }
}
