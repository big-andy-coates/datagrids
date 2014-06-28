package org.acc.coherence.versioning.temporal;

/**
 * Created by andy on 28/06/2014.
 */
public interface Versioned<DomainType> {
    // Pof interface:
    final static int DOMAIN_POF_ID = 2;

    int getVersion();

    void setVersion(int version);

    DomainType getDomainObject();
}
