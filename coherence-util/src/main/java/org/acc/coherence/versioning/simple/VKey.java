package org.acc.coherence.versioning.simple;

import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;

/**
 * Wrapper type for versioned keys
 */
@Portable
public class VKey<DomainKey> implements Versioned<DomainKey> {
    @PortableProperty(value = VERSION_POF_ID)
    private int version;

    @PortableProperty(value = DOMAIN_POF_ID)
    private DomainKey domainKey;

    @Override
    public int getVersion() {
        return version;
    }

    @Override
    public DomainKey getDomainObject() {
        return domainKey;
    }
}
