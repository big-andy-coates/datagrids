package org.acc.coherence.service;

import com.oracle.coherence.common.liveobjects.*;
import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.ResourceRegistry;
import org.acc.cluster.service.ClusterService;
import org.acc.cluster.service.ClusterServiceException;
import org.acc.cluster.service.ClusterServicesController;
import org.acc.cluster.service.ServiceId;

import java.io.Serializable;

/**
 * @author datalorax - 14/11/2014.
 */
@LiveObject
@Portable
public class ClusterServiceHolder<ServiceData> implements Serializable {
    private static final long serialVersionUID = -4003608335782280033L;

    @PortableProperty(value = 1)
    private Class<? extends ClusterService<ServiceData>> serviceType;

    @PortableProperty(value = 1)
    private ServiceData serviceData;

    @SuppressWarnings("UnusedDeclaration")      // Used by Coherence
    @Deprecated                                 // Only
    public ClusterServiceHolder() {
    }

    public ClusterServiceHolder(Class<? extends ClusterService<ServiceData>> serviceType, ServiceData serviceData) {
        this.serviceType = serviceType;
        this.serviceData = serviceData;
    }

    @SuppressWarnings("UnusedDeclaration")  // Called by Coherence
    @OnInserting
    public void onInserting(BinaryEntry entry) {
        final ServiceId serviceId = getServiceId(entry);
        final CoherenceClusterServicesController servicesManager = getServiceManager(entry);
        servicesManager.registerService(serviceType, serviceId, serviceData);
    }

    @SuppressWarnings("UnusedDeclaration")  // Called by Coherence
    @OnInserted
    public void onInserted(BinaryEntry entry) {
        final ServiceId serviceId = getServiceId(entry);
        final CoherenceClusterServicesController servicesManager = getServiceManager(entry);
        servicesManager.startService(serviceType, serviceId);
    }

    @SuppressWarnings({"UnusedDeclaration"})  // Called by Coherence
    @OnUpdating
    public void onUpdating(BinaryEntry entry) {
        // Todo(ac): need to protect state from unexpected / acknowledged change from external system - or pass it to service
    }

    @SuppressWarnings("UnusedDeclaration")  // Called by Coherence
    @OnRemoving
    public void onRemoving(BinaryEntry entry) {
        final ServiceId serviceId = getServiceId(entry);
        final CoherenceClusterServicesController servicesManager = getServiceManager(entry);
        servicesManager.stopService(serviceType, serviceId);
    }

    @SuppressWarnings("UnusedDeclaration")  // Called by Coherence
    @OnRemoved
    public void onRemoved(BinaryEntry entry) {
        final ServiceId serviceId = getServiceId(entry);
        final CoherenceClusterServicesController servicesManager = getServiceManager(entry);
        servicesManager.unregisterService(serviceType, serviceId);
    }

    @Override
    public String toString() {
        return "ClusterServiceHolder{serviceType=" + serviceType + ", serviceData=" + serviceData + '}';
    }

    private static CoherenceClusterServicesController getServiceManager(BinaryEntry entry) {
        final ResourceRegistry registry = entry.getContext().getManager().getCacheFactory().getResourceRegistry();
        return registry.getResource(CoherenceClusterServicesController.class);
    }

    private static ServiceId getServiceId(BinaryEntry entry) {
        Object key = entry.getKey();
        if (!(key instanceof ServiceId)) {
            throw new ClusterServiceException("Invalid key. Key must be instance of ServiceId. Actual key: " + key);
        }
        return (ServiceId)key;
    }
}
