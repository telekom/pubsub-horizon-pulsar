package de.telekom.horizon.pulsar.service;

import de.telekom.eni.pandora.horizon.cache.service.DeDuplicationService;
import de.telekom.eni.pandora.horizon.kafka.event.EventWriter;
import de.telekom.eni.pandora.horizon.kubernetes.resource.SubscriptionResource;
import de.telekom.eni.pandora.horizon.mongo.repository.MessageStateMongoRepo;
import de.telekom.eni.pandora.horizon.tracing.HorizonTracer;
import de.telekom.horizon.pulsar.cache.ConnectionCache;
import de.telekom.horizon.pulsar.cache.ConnectionGaugeCache;
import de.telekom.horizon.pulsar.config.PulsarConfig;
import de.telekom.horizon.pulsar.helper.SseTaskStateContainer;
import de.telekom.horizon.pulsar.utils.KafkaPicker;
import io.fabric8.kubernetes.client.KubernetesClient;
import lombok.Getter;
import org.springframework.stereotype.Component;

/**
 * Factory class for creating Server-Sent Event (SSE) tasks.
 *
 * This class, annotated with {@code @Getter} and {@code @Component}, serves as a factory for creating
 * {@link SseTask} instances. It is responsible for initializing and providing necessary dependencies
 * such as configuration, Kubernetes client, connection cache, etc. It also includes a method for creating
 * a new SSE task for a given subscription.
 */
@Getter
@Component
public class SseTaskFactory {
    private final PulsarConfig pulsarConfig;
    private final KubernetesClient kubernetesClient;
    private final ConnectionCache connectionCache;
    private final ConnectionGaugeCache connectionGaugeCache;
    private final KafkaPicker kafkaPicker;
    private final MessageStateMongoRepo messageStateMongoRepo;
    private final DeDuplicationService deDuplicationService;
    private final HorizonTracer tracingHelper;
    private final EventWriter eventWriter;

    /**
     * Constructs an instance of {@code SseTaskFactory}.
     *
     * @param pulsarConfig           The {@link PulsarConfig} for Pulsar-related configuration.
     * @param kubernetesClient       The {@link KubernetesClient} for interacting with Kubernetes resources.
     * @param connectionCache        The {@link ConnectionCache} for managing connections.
     * @param connectionGaugeCache   The {@link ConnectionGaugeCache} for caching connection gauges.
     * @param kafkaPicker            The {@link KafkaPicker} for picking Kafka events.
     * @param messageStateMongoRepo  The {@link MessageStateMongoRepo} for interacting with message states in MongoDB.
     * @param deDuplicationService   The {@link DeDuplicationService} for handling message deduplication.
     * @param tracingHelper          The {@link HorizonTracer} for tracing and adding spans to the execution flow.
     * @param eventWriter            The {@link EventWriter} for writing events.
     */
    public SseTaskFactory(
            PulsarConfig pulsarConfig,
            KubernetesClient kubernetesClient,
            ConnectionCache connectionCache,
            ConnectionGaugeCache connectionGaugeCache,
            EventWriter eventWriter,
            KafkaPicker kafkaPicker,
            MessageStateMongoRepo messageStateMongoRepo,
            DeDuplicationService deDuplicationService,
            HorizonTracer tracingHelper) {

        this.pulsarConfig = pulsarConfig;
        this.connectionGaugeCache = connectionGaugeCache;
        this.kafkaPicker = kafkaPicker;
        this.messageStateMongoRepo = messageStateMongoRepo;
        this.tracingHelper = tracingHelper;
        this.kubernetesClient = kubernetesClient;
        this.connectionCache = connectionCache;
        this.deDuplicationService = deDuplicationService;

        this.eventWriter = eventWriter;
    }

    /**
     * Creates a new {@link SseTask} for the specified subscription and content type.
     *
     * @param environment               The environment associated with the subscription.
     * @param subscriptionId            The ID of the subscription for which the task is created.
     * @param contentType               The content type for the SSE task.
     * @param sseTaskStateContainer     The {@link SseTaskStateContainer} representing the state of the SSE task.
     * @param includeHttpHeaders        A boolean flag indicating whether to include HTTP headers in the SSE task.
     * @return The newly created {@link SseTask}.
     */
    public SseTask createNew(String environment, String subscriptionId, String contentType, SseTaskStateContainer sseTaskStateContainer, boolean includeHttpHeaders) {
        var eventMessageSupplier = new EventMessageSupplier(subscriptionId, this, includeHttpHeaders);
        var connection = connectionGaugeCache.getOrCreateGaugeForSubscription(environment, subscriptionId);

        var task = new SseTask(sseTaskStateContainer, eventMessageSupplier, connection, this);
        task.setContentType(contentType);

        claimConnectionForSubscription(environment, subscriptionId, task);

        return task;
    }

    /**
     * Claims the connection for the specified subscription by updating Kubernetes resources and connection cache.
     *
     * @param environment      The environment associated with the subscription.
     * @param subscriptionId   The ID of the subscription for which the connection is claimed.
     * @param connection       The {@link SseTask} representing the connection.
     */
    private void claimConnectionForSubscription(String environment, String subscriptionId, SseTask connection) {
        var resource = kubernetesClient.resources(SubscriptionResource.class)
                .inNamespace(pulsarConfig.getNamespace())
                .withName(subscriptionId).get();

        if (resource != null) {
            resource.getSpec().setSseActiveOnPod(pulsarConfig.getPodName());

            kubernetesClient.resources(SubscriptionResource.class).inNamespace(pulsarConfig.getNamespace()).replace(resource);

            connectionCache.addConnectionForSubscription(environment, subscriptionId, connection);
        }
    }
}
