package org.acc.coherence.versioning.temporal;

import com.tangosol.net.PartitionedService;
import com.tangosol.net.partition.KeyAssociator;

/**
 * Created by andy on 28/06/2014.
 */
public class VersionedKeyAssociator implements KeyAssociator {
    @Override
    public void init(PartitionedService partitionedService) {
    }

    @Override
    public Object getAssociatedKey(Object key) {
        if (key instanceof VKey) {
            return ((VKey) key).getDomainObject();
        }

        return null;
    }
}
