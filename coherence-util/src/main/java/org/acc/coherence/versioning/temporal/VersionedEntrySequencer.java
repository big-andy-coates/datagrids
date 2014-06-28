package org.acc.coherence.versioning.temporal;

import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.net.BackingMapContext;
import com.tangosol.util.Binary;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.ExternalizableHelper;
import org.apache.commons.lang3.Validate;

import java.util.Map;

/**
 * Created by andy on 28/06/2014.
 */
@Portable
public class VersionedEntrySequencer implements EntrySequencer {
    public VersionedEntrySequencer() {
    }

    @Override
    public Map.Entry getPrevious(Map.Entry entry, BackingMapContext context) {
        return getNextKey(entry, context, -1);
    }

    @Override
    public Map.Entry getNext(Map.Entry entry, BackingMapContext context) {
        Validate.isInstanceOf(VKey.class, entry.getKey());
        return getNextKey(entry, context, 1);
    }

    private Map.Entry getNextKey(Map.Entry entry, BackingMapContext context, int offset) {
        Validate.isInstanceOf(VKey.class, entry.getKey());

        final VKey vKey = (VKey) entry.getKey();
        final int version = vKey.getVersion();
        if (offset < 0 && version == VKey.FIRST_VERSION) {
            return null;    // No previous
        }

        final VKey<Object> nextKey = new VKey<Object>(vKey.getDomainObject(), version + offset);
        if (!(entry instanceof BinaryEntry)) {
            return context.getReadOnlyEntry(nextKey);
        }

        final BinaryEntry binaryEntry = (BinaryEntry) entry;
        final Binary undecorated = serialiseKey(nextKey, context);
//        InvocableMap.Entry entry1 = context.getReadOnlyEntry(nextKey);  // todo(ac): would be nice to be able to do this - but unsupported within index.
        // Todo(ac): due to decoration, we're seeing mismatch between bKey and actual map entries, (and hence index is not working). For now we loop - but this is O(n) - not good!
        // Todo(ac): might need a domainKey index adding first and use that to restrict this loop e.g.
//        Map<ValueExtractor,MapIndex> indexMap = binaryEntry.getBackingMapContext().getIndexMap();
//        MapIndex keyIndex = indexMap.get(VKey.DOMAIN_POF_EXTRACTED);
//        Set<Binary> keys  = (Set<Binary>) keyIndex.get(vKey.getDomainObject());
//        for (Binary binary : keys) {
//            Binary naked = (Binary)ExternalizableHelper.removeIntDecoration((ReadBuffer)binary);
//            if (naked.equals(undecorated)) {
//                return binary;
//            }
//        }

        Map<Binary, Binary> backingMap = binaryEntry.getBackingMap();
        for (Map.Entry<Binary, Binary> e : backingMap.entrySet()) {
            Binary naked = ExternalizableHelper.removeIntDecoration(e.getKey());
            if (naked.equals(undecorated)) {
                return e;
            }
        }
        return null;    // Not found
    }

    private Binary serialiseKey(VKey key, BackingMapContext context) {
        return ExternalizableHelper.toBinary(key, context.getManagerContext().getCacheService().getSerializer());
    }
}
