// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.pulsar.helper;


import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Class that contains any streaming limits provided by the customer.
 *
 * This class encapsulates all possible streaming limits that have been provided by the customer when
 * requesting a new stream. The streaming limits will ensure that a active stream terminates early on when exceeded.
 * Currently, a customer can specify that the stream should terminate when a specific number of events have been consumed
 * or after a certain time (in minutes) or after exceeding a certain number of bytes which have been consumed.
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class StreamLimit {
    private int maxNumber;
    private int maxMinutes;
    private long maxBytes;

    public static StreamLimit of(int maxNumber, int maxMinutes, int maxBytes) {
        return new StreamLimit(maxNumber, maxMinutes, maxBytes);
    }
}
