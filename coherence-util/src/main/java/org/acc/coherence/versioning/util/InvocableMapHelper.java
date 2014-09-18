package org.acc.coherence.versioning.util;

import com.tangosol.io.Serializer;
import com.tangosol.io.pof.PofContext;
import com.tangosol.io.pof.reflect.PofNavigator;
import com.tangosol.io.pof.reflect.PofValue;
import com.tangosol.io.pof.reflect.PofValueParser;
import com.tangosol.util.Binary;
import com.tangosol.util.ExternalizableHelper;
import com.tangosol.util.ValueExtractor;
import com.tangosol.util.extractor.PofExtractor;

/**
 * Created 09/09/2014
 *
 * @author Andy Coates.
 */
public class InvocableMapHelper extends com.tangosol.util.InvocableMapHelper {
    public static Object extractFromObject(ValueExtractor extractor, Object object, Serializer serialiser) {
        if (extractor instanceof PofExtractor) {
            PofNavigator navigator = ((PofExtractor) extractor).getNavigator();
            PofValue pofValue = PofValueParser.parse((Binary) object, (PofContext) serialiser);
            return navigator.navigate(pofValue).getValue();
        }

        if (object instanceof Binary) {
            Object deserialised = ExternalizableHelper.fromBinary((Binary)object, serialiser);
            return extractor.extract(deserialised);
        }

        return extractor.extract(object);
    }
}
