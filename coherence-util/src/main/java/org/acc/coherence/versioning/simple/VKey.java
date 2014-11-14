package org.acc.coherence.versioning.simple;

import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;

import java.io.Serializable;

/**
 * Wrapper type for versioned keys. Note, for testing this supports both Pof and Java serialisation.
 * @author datalorax - 14/11/2014.
 */
@Portable
public class VKey<DomainKey> implements Versioned<DomainKey>, Serializable {
    private static final long serialVersionUID = 2135400483405530953L;

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
