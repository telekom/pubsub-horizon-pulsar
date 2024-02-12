package de.telekom.horizon.pulsar.api;

import de.telekom.eni.pandora.horizon.kubernetes.resource.Subscription;
import de.telekom.eni.pandora.horizon.kubernetes.resource.SubscriptionResource;
import de.telekom.eni.pandora.horizon.kubernetes.resource.SubscriptionResourceSpec;
import de.telekom.horizon.pulsar.cache.SubscriberCache;
import de.telekom.horizon.pulsar.exception.SubscriberDoesNotMatchSubscriptionException;
import de.telekom.horizon.pulsar.service.SseService;
import de.telekom.horizon.pulsar.utils.AbstractIntegrationTest;
import de.telekom.horizon.pulsar.utils.MockedKubernetesConfiguration;
import de.telekom.horizon.pulsar.utils.MongoTestServerConfiguration;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.context.annotation.Import;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.head;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@Import({MockedKubernetesConfiguration.class, MongoTestServerConfiguration.class})
class SseControllerSecurityEnabledTest extends AbstractIntegrationTest {

    @Autowired
    KubernetesClient client;

    @Autowired
    public MockMvc mockMvc;

    @Value("${pulsar.security.issuerUrls}")
    private String issuerUrl;

    @SpyBean
    private SseService sseService;

    @Autowired
    private SubscriberCache subscriberCache;

    @Test
    void headRequestReturns204() throws Exception {
        mockMvc.perform(head("/v1/integration/sse"))
                .andExpect(status().isNoContent())
                .andExpect(header().exists("X-Health-Check-Timestamp"));
    }

    @Test
    void getReturnsForbiddenIfSubscriberValidationFails() throws Exception {
        String env = "integration";
        String subscriptionId = "subscriptionId";
        String subscriberId = "subscriberId";

        var jwt = getJwt(subscriberId);

        // when / then
        mockMvc.perform(get("/v1/integration/sse/"+subscriptionId)
                        .with(jwt().jwt(jwt)))
                .andExpect(status().isForbidden())
                .andExpect(result -> assertInstanceOf(SubscriberDoesNotMatchSubscriptionException.class, result.getResolvedException()));

        verify(sseService, times(1)).validateSubscriberIdForSubscription(eq(env), eq(subscriptionId));
    }

    @Test
    void getEventsViaSSESuccessfullyWithOk() throws Exception {
        // given
        String env = "integration";
        String subscriptionId = "subscriptionId";
        String subscriberId = "subscriberId";

        var jwt = getJwt(subscriberId);
        subscriberCache.add(env, subscriptionId, subscriberId);
        createKubernetesSubscriptionResource(env, subscriptionId, subscriberId);

        // when / then
        mockMvc.perform(get("/v1/integration/sse/"+subscriptionId).queryParam("includeHttpHeaders", "true")
                        .with(jwt().jwt(jwt)))
                .andExpect(status().isOk());
        verify(sseService, times(1)).validateSubscriberIdForSubscription(eq(env), eq(subscriptionId));
        verify(sseService, times(1)).startEmittingEvents(eq(env), eq(subscriptionId), any(String.class), eq(true));

        // cleanup
        subscriberCache.remove(env, subscriptionId);
    }

    private void createKubernetesSubscriptionResource(String env, String subscriptionId, String subscriberId) {
        var crd = CustomResourceDefinitionContext.v1CRDFromCustomResourceType(SubscriptionResource.class).build();
        client.apiextensions().v1().customResourceDefinitions().create(crd);
        SubscriptionResource resource = new SubscriptionResource();
        ObjectMeta objectMeta = new ObjectMeta();
        objectMeta.setName(subscriptionId);
        resource.setMetadata(objectMeta);

        SubscriptionResourceSpec spec = new SubscriptionResourceSpec();
        spec.setEnvironment(env);
        spec.setSseActiveOnPod("pod01");

        Subscription subscription = new Subscription();
        subscription.setSubscriptionId(subscriptionId);
        subscription.setDeliveryType("SSE");
        subscription.setType("testevent");
        subscription.setSubscriberId(subscriberId);

        spec.setSubscription(subscription);

        resource.setSpec(spec);
        client.resources(SubscriptionResource.class).inNamespace("integration").create(resource);
    }

    private Jwt getJwt(String subscriberId) {
        return Jwt.withTokenValue("token")
                .header("alg", "none")
                .claim("clientId", subscriberId)
                .claim("iss", issuerUrl)
                .build();
    }

    @DynamicPropertySource
    static void dynamicProperties(DynamicPropertyRegistry registry) {
        registry.add("pulsar.security.oauth", () -> true);
    }

    @BeforeAll
    static void setUp() {
        MockedKubernetesConfiguration.server.before();
    }

    @AfterAll
    static void tearDown() {
        MockedKubernetesConfiguration.server.after();
    }

}