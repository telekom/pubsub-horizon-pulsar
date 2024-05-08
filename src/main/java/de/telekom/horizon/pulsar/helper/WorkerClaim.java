package de.telekom.horizon.pulsar.helper;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serial;
import java.io.Serializable;

@Data
@AllArgsConstructor
public class WorkerClaim implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private String subscriptionId;
}
