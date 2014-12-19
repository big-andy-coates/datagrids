package org.acc.coherence.service;

import com.tangosol.util.Builder;
import com.tangosol.util.RegistrationBehavior;
import com.tangosol.util.ResourceRegistry;
import org.acc.cluster.service.*;

import java.lang.reflect.Constructor;

/**
 * Coherence specific implementation of the ClusterServiceController. The controller makes use of the Coherence
 * ResourceRegistry to register service instances with.
 *
 * @author Andy - 14/11/2014.
 */
public class CoherenceClusterServicesController implements ClusterServicesController {
    private final ResourceRegistry resourceRegistry;

    public CoherenceClusterServicesController(ResourceRegistry resourceRegistry) {
        this.resourceRegistry = resourceRegistry;
    }

    public <Service extends ClusterService<ServiceData>, ServiceData>
    void registerService(Class<Service> serviceType, ServiceId serviceId, ServiceData serviceData) {
        resourceRegistry.registerResource(serviceType, serviceId.getId(), new Builder<Service>() {
            @Override
            public Service realize() {
                return instantiate(serviceType, serviceId, serviceData);
            }
        }, RegistrationBehavior.FAIL, null);  // Todo(ac): RegistrationBehaviour should be customisable...
    }

    @Override
    public <Service extends ClusterService<?>> void startService(Class<Service> serviceType, ServiceId serviceId) {
        final Service service = resourceRegistry.getResource(serviceType, serviceId.getId());
        if (service == null) {
            throw new ClusterServiceException("Service is not registered: " + serviceId);
        }

        service.start();
    }

    public <Service extends ClusterService<?>> void stopService(Class<Service> serviceType, ServiceId serviceId) {
        // Todo: same optionally fail on stop if not present.
        final Service service = resourceRegistry.getResource(serviceType, serviceId.getId());
        if (service != null) {
            service.stop(false);
        }
    }

    public <Service extends ClusterService<?>> void unregisterService(Class<Service> serviceType, ServiceId serviceId) {
        // Todo: same optionally fail on shutdown if not present.
        final Service service = resourceRegistry.getResource(serviceType, serviceId.getId());
        resourceRegistry.unregisterResource(serviceType, serviceId.getId());
        if (service != null) {
            service.stop(true);
        }
    }

    private <Service extends ClusterService<ServiceData>, ServiceData>
    Service instantiate(Class<Service> serviceType, ServiceId serviceId, ServiceData serviceData) {
        try {
            Constructor<Service> constructor = serviceType.getDeclaredConstructor(ServiceId.class, ClusterServiceContext.class);
            return constructor.newInstance(serviceId, new CoherenceServiceContext<ServiceData>(this, null, serviceData));  // Todo(ac): executor
        } catch (NoSuchMethodException e) {
            throw new ClusterServiceException("No appropriate constructor found for cluster service type: " + serviceType, e);
        } catch (Exception e) {
            throw new ClusterServiceException("Failed to instantiate cluster service type: " + serviceType, e);
        }
    }
}
