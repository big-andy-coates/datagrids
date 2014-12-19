package org.acc.cluster.service;

/**
 * Created by Andy on 18/12/2014.
 */
public class ClusterServiceException extends RuntimeException {
    public ClusterServiceException(String message) {
        super(message);
    }

    public ClusterServiceException(String message, Throwable cause) {
        super(message, cause);
    }
}
