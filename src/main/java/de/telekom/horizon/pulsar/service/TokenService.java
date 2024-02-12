package de.telekom.horizon.pulsar.service;

/**
 * Interface representing a service for retrieving subscriberIds.
 *
 * This interface defines a method {@code getSubscriberId()} to obtain the subscriberId associated
 * with the current authentication token. Implementations of this interface provide functionality
 * for extracting subscriberIds from authentication tokens, allowing components to retrieve the
 * identity of the subscriber making requests.
 */
public interface TokenService {

    /**
     * Retrieves the subscriberId associated with the current authentication token.
     *
     * @return The subscriberId, or {@code null} if the subscriberId cannot be determined.
     */
    String getSubscriberId();
}
