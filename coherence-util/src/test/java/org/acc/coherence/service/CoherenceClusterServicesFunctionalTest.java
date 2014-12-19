package org.acc.coherence.service;

import com.tangosol.net.CacheFactory;
import com.tangosol.net.InvocationService;
import org.acc.cluster.service.ClusterService;
import org.acc.cluster.service.ClusterServiceContext;
import org.acc.cluster.service.ServiceId;
import org.acc.coherence.singleton.ClusterSingletonService;
import org.acc.coherence.test.ClusterBasedTest;
import org.hamcrest.Matcher;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Set;

import static org.acc.coherence.singleton.ClusterSingletons.startSingleton;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class CoherenceClusterServicesFunctionalTest extends ClusterBasedTest {
    private static final String START_TRACKING_CACHE = "test-start-tracking-CoherenceClusterServicesFunctionalTest";
    private static final String STOP_TRACKING_CACHE = "test-stop-tracking-CoherenceClusterServicesFunctionalTest";
    private final Set<ServiceId> startedServices = new HashSet<ServiceId>();

    public CoherenceClusterServicesFunctionalTest() {
        super("org/acc/coherence/service/service-coherence-cache-config.xml");
    }

    @BeforeClass
    public void beforeClass() {
        super.beforeClass();
        ensureServiceController();
        ensureResultsCaches();
    }

    @AfterMethod
    public void tearDown() {
        super.tearDown();

        stopAllServices();

        clearCaches(START_TRACKING_CACHE, STOP_TRACKING_CACHE);
    }

    @Test
    public void shouldStartService() throws Exception {
        // Given:
        final ServiceId serviceId = new ServiceId("bob");

        // When:
        whenServiceStarted(serviceId, TestServiceClass.class, "testData");

        // Then:
        assertThat(getTime(START_TRACKING_CACHE, serviceId), is(validTime()));
    }

    @Test
    public void shouldNotCallStopWhenStarting() throws Exception {
        // Given:
        final ServiceId serviceId = new ServiceId("peter");

        // When:
        whenServiceStarted(serviceId, TestServiceClass.class, "testData");

        // Then:
        assertThat(getTime(STOP_TRACKING_CACHE, serviceId), is(nullValue()));
    }

    @Test
    public void shouldStopService() throws Exception {
        // Given:
        final ServiceId serviceId = new ServiceId("jane");
        givenServiceStarted(serviceId, TestServiceClass.class, "testData");

        // When:
        whenServiceStopped(serviceId);

        // Then:
        // Todo(ac): fails as start / stop is async. Need CacheWaitable
        assertThat(getTime(STOP_TRACKING_CACHE, serviceId), is(validTime()));
    }

    // Todo(ac): extend this set of tests.
    // Todo(ac): put in functionality to allow service to update its own state in the cluster.
    // Todo(ac): test partition moves.
    // Todo(ac): move singleton service over to just sit over the top of service.
    // Happy Days.

    private <ServiceData> void givenServiceStarted(ServiceId serviceId,
                                                  Class<? extends ClusterService<ServiceData>> serviceType,
                                                  ServiceData serviceData) {
        whenServiceStarted(serviceId, serviceType, serviceData);
    }

    private <ServiceData> void whenServiceStarted(ServiceId serviceId,
                                                  Class<? extends ClusterService<ServiceData>> serviceType,
                                                  ServiceData serviceData) {
        startedServices.add(serviceId);
        ClusterServices.ensureService(serviceId, serviceType, serviceData);
    }

    private void whenServiceStopped(ServiceId serviceId) {
        startedServices.remove(serviceId);
        ClusterServices.stopService(serviceId);
    }

    private static void ensureServiceController() {
        final InvocationService service = (InvocationService) CacheFactory.getService("invocation-service");
        service.execute(new ControllerInstallerInvocable(), service.getCluster().getMemberSet(), null);
    }

    private static void ensureResultsCaches() {
        CacheFactory.getCache(START_TRACKING_CACHE);
        CacheFactory.getCache(STOP_TRACKING_CACHE);
    }

    private static Long getTime(String timeType, ServiceId serviceId) {
        return (Long) CacheFactory.getCache(timeType).get(serviceId);
    }

    private static Matcher<Long> validTime() {
        return both(greaterThan(0L)).and(lessThanOrEqualTo(System.currentTimeMillis()));
    }

    private void stopAllServices() {
        startedServices.forEach(ClusterServices::stopService);
    }

    public static class TestServiceClass implements ClusterService<String> {
        private final ServiceId serviceId;
        private ClusterServiceContext<String> context;

        public TestServiceClass(ServiceId serviceId, ClusterServiceContext<String> context) {
            this.serviceId = serviceId;
            this.context = context;
        }

        @Override
        public void start() {
            CacheFactory.getCache(START_TRACKING_CACHE).put(serviceId, System.currentTimeMillis());
        }

        @Override
        public void stop(boolean block) {
            if (block) {
                CacheFactory.getCache(START_TRACKING_CACHE).remove(serviceId);
            }
        }

        @Override
        public ClusterServiceContext<String> getContext() {
            return context;
        }
    }
}