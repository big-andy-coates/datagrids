package org.acc.cluster.service;

import java.io.Serializable;

/**
 * Created by Andy on 18/12/2014.
 */
public final class ServiceId implements Serializable {
    private static final long serialVersionUID = -4885776789930593952L;

    private final String id;

    public ServiceId(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }
}
