package org.acc.coherence.config.example.custom.scheme;

import com.tangosol.coherence.config.xml.processor.CustomizableBuilderProcessor;
import com.tangosol.config.xml.AbstractNamespaceHandler;

/**
 * @author jk & acoates 2014.09.24
 */
public class CustomNamespaceHandler extends AbstractNamespaceHandler {
    public CustomNamespaceHandler() {
        // Register our custom local scheme's processor:
        registerProcessor("custom-local-scheme", new CustomizableBuilderProcessor<>(CustomLocalScheme.class));
    }
}
