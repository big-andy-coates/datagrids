package org.acc.coherence.service;

import com.tangosol.net.CacheFactory;
import com.tangosol.net.NamedCache;
import org.acc.cluster.service.ClusterService;
import org.acc.cluster.service.ServiceId;
import org.apache.commons.lang3.Validate;

import java.util.Set;

/**
 * @author Andy - 14/11/2014.
 */
public final class ClusterServices {
    public static final String SERVICES_CACHE_NAME = "acc.cluster.services";

    public static <ServiceData> void ensureService(ServiceId serviceId,
                                                   Class<? extends ClusterService<ServiceData>> serviceClass,
                                                   ServiceData initialServiceData) {
        getServicesCache().put(serviceId, new ClusterServiceHolder<ServiceData>(serviceClass, initialServiceData));
    }

    public static boolean stopService(ServiceId serviceId) {
        return getServicesCache().remove(serviceId) != null;
    }

    public static boolean isRunning(ServiceId serviceId) {
        return getServicesCache().containsKey(serviceId);
    }

    @SuppressWarnings("unchecked")
    public static Set<ServiceId> getRunningServiceIds() {
        return getServicesCache().keySet();
    }

    private static NamedCache getServicesCache() {
        return CacheFactory.getCache(SERVICES_CACHE_NAME);
    }

    private ClusterServices() {
    }
}
