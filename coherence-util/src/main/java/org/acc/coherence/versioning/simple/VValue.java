package org.acc.coherence.versioning.simple;

import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;

/**
 * Wrapper type for versioned values.
 */
@Portable
public class VValue<DomainValue> implements Versioned<DomainValue> {
    @PortableProperty(value = VERSION_POF_ID)
    private int version;

    @PortableProperty(value = DOMAIN_POF_ID)
    private DomainValue domainValue;

    @Override
    public int getVersion() {
        return version;
    }

    @Override
    public DomainValue getDomainObject() {
        return domainValue;
    }
}
