package org.acc.cluster.service;

import java.util.concurrent.ExecutorService;

/**
 * @author Andy - 14/11/2014.
 */
public interface ClusterServiceContext<ServiceData> {
    ClusterServicesController getController();
    ExecutorService getExecutorService();
    ServiceData getServiceData();
    void saveServiceData();
}
