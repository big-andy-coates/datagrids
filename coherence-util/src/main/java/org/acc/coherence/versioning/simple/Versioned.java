package org.acc.coherence.versioning.simple;

/**
 * Created by andy on 28/06/2014.
 */
public interface Versioned<DomainType> {
    // Pof interface:
    final static int VERSION_POF_ID = 1;
    final static int DOMAIN_POF_ID = 2;

    int getVersion();

    DomainType getDomainObject();
}
