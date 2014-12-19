package org.acc.coherence.service;

import com.tangosol.net.*;
import com.tangosol.util.Builder;
import com.tangosol.util.RegistrationBehavior;
import com.tangosol.util.ResourceRegistry;
import org.acc.cluster.service.ClusterServicesController;

/**
 * Simple way of installing the controller without a custom main on the storage nodes. Ideal for testing.
 */
public class ControllerInstallerInvocable extends AbstractInvocable {
    private static final long serialVersionUID = 3903232397575084685L;

    @Override
    public void run() {
        if (isStorageDisabled()) {
            return;
        }

        final ResourceRegistry resourceRegistry = CacheFactory.getConfigurableCacheFactory().getResourceRegistry();
        resourceRegistry.registerResource(
                CoherenceClusterServicesController.class, new Builder<CoherenceClusterServicesController>() {
                    @Override
                    public CoherenceClusterServicesController realize() {
                        return new CoherenceClusterServicesController(resourceRegistry);
                    }
                },
                RegistrationBehavior.IGNORE, null);
    }

    private boolean isStorageDisabled() {
        NamedCache cache = CacheFactory.getCache(ClusterServices.SERVICES_CACHE_NAME);
        DistributedCacheService service = (DistributedCacheService) cache.getCacheService();
        return !service.isLocalStorageEnabled();
    }
}
