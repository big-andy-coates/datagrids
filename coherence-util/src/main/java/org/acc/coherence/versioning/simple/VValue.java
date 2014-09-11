package org.acc.coherence.versioning.simple;

import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;

import java.io.Serializable;

/**
 * Wrapper type for versioned values. Note, for testing this supports both Pof and Java serialisation.
 */
@Portable
public class VValue<DomainValue> implements Versioned<DomainValue>, Serializable {
    private static final long serialVersionUID = 1874340015425351168L;

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
