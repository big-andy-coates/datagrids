package org.acc.coherence.versioning.temporal;

import com.tangosol.net.BackingMapContext;

import java.util.Map;

/**
 * Simple interface to allow custom sequencing of map entries (on local node only).
 */
public interface EntrySequencer {
    Map.Entry getPrevious(Map.Entry entry, BackingMapContext context);

    Map.Entry getNext(Map.Entry entry, BackingMapContext context);
}
