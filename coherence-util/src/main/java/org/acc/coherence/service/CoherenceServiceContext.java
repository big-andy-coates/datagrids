package org.acc.coherence.service;

import org.acc.cluster.service.ClusterServiceContext;
import org.acc.cluster.service.ClusterServicesController;

import java.util.concurrent.ExecutorService;

/**
 * Created by Andy on 18/12/2014.
 */
public class CoherenceServiceContext<ServiceData> implements ClusterServiceContext<ServiceData>{
    private final ClusterServicesController controller;
    private final ExecutorService executor;
    private final ServiceData serviceData;

    public CoherenceServiceContext(ClusterServicesController controller, ExecutorService executor, ServiceData serviceData) {
        this.controller = controller;
        this.executor = executor;
        this.serviceData = serviceData;
    }

    @Override
    public ClusterServicesController getController() {
        return controller;
    }

    @Override
    public ExecutorService getExecutorService() {
        return executor;
    }

    @Override
    public ServiceData getServiceData() {
        return serviceData;
    }

    @Override
    public void saveServiceData() {
        // Todo(ac): save to cache
    }
}
