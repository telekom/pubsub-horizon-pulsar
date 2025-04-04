// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.pulsar.helper;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serial;
import java.io.Serializable;
import java.util.UUID;

@Data
@AllArgsConstructor
public class WorkerClaim implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private String subscriptionId;
    private UUID workerId;
}
