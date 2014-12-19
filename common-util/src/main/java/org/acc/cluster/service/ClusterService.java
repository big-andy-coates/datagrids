package org.acc.cluster.service;

/**
 * Classes implementing ClusterService must also provide a public constructor that takes the following parameters:
 * (ServiceId, ClusterServiceContext<ServiceData>).
 * @author Andy - 14/11/2014.
 */
public interface ClusterService<ServiceData> {
    // Required constructor: ClusterService(ServiceId, ClusterServiceContext<ServiceData>)

    void start();
    void stop(boolean block);
    ClusterServiceContext<ServiceData> getContext();
}
