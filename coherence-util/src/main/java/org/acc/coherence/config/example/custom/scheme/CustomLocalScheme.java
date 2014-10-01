package org.acc.coherence.config.example.custom.scheme;

import com.tangosol.coherence.config.ParameterList;
import com.tangosol.coherence.config.ResolvableParameterList;
import com.tangosol.coherence.config.builder.ParameterizedBuilder;
import com.tangosol.coherence.config.scheme.LocalScheme;
import com.tangosol.config.annotation.Injectable;
import com.tangosol.config.expression.ChainedParameterResolver;
import com.tangosol.config.expression.Expression;
import com.tangosol.config.expression.NullParameterResolver;
import com.tangosol.config.expression.ParameterResolver;
import com.tangosol.config.injection.Injector;
import com.tangosol.config.injection.SimpleInjector;
import com.tangosol.net.BackingMapManagerContext;
import com.tangosol.net.cache.LocalCache;
import com.tangosol.util.ResourceResolver;
import com.tangosol.util.ResourceResolverHelper;

/**
 * An extension of {@link LocalScheme} that will realize
 * instances of a custom class that extends {@link LocalCache}.
 *
 * @author jk & acoates 2014.09.24
 */
public class CustomLocalScheme
        extends LocalScheme
        implements ParameterizedBuilder<LocalCache> {

    private String exampleCustomStringParam;
    private Long exampleCustomLongParam;
    private Expression<BackingMapManagerContext> exampleCustomMacroParam;

    /**
     * An example custom injectable.
     * @param param the custom string.
     */
    @SuppressWarnings("UnusedDeclaration")  // Called by Coherence
    @Injectable(value = "example-custom-string-param")
    public void setOurExampleStringParam(String param) {
        exampleCustomStringParam = param;
    }

    /**
     * An example custom injectable demonstrating type coercion.
     * @param param
     */
    @SuppressWarnings("UnusedDeclaration")  // Called by Coherence
    @Injectable(value = "example-custom-long-param")
    public void setOurExampleLongParam(long param) {
        exampleCustomLongParam = param;
    }

    /**
     * An example injectable that captures one of the standard macro expressions.
     * This could just as easily be captured as a injectable on the custom local cache that accepts a pure BackingMapManagerContext
     * But is included here as an example of an alternative.
     * @param param the macro expression, which can evaluated when instances of the cache are created in the realise method
     */
    @SuppressWarnings("UnusedDeclaration")  // Called by Coherence
    @Injectable(value = "example-custom-macro-param")
    public void setOutExampleMacroParam(Expression<BackingMapManagerContext> param) {
        this.exampleCustomMacroParam = param;
    }

    @Override
    public ParameterizedBuilder<LocalCache> getCustomBuilder() {
        return this;
    }

    @Override
    public LocalCache realize(ParameterResolver parameterResolver, ClassLoader loader, ParameterList listParameters) {

        final CustomLocalCache cache = instantiateCache(parameterResolver);

        initialCacheWithStandardInjectables(cache, parameterResolver, listParameters);

        return cache;
    }

    private CustomLocalCache instantiateCache(ParameterResolver parameterResolver) {
        final BackingMapManagerContext ctx = exampleCustomMacroParam == null ? null : exampleCustomMacroParam.evaluate(parameterResolver);
        return new CustomLocalCache(exampleCustomStringParam, exampleCustomLongParam, ctx);
    }

    private void initialCacheWithStandardInjectables(CustomLocalCache cache, ParameterResolver parameterResolver, ParameterList listParameters) {
        final ChainedParameterResolver resolver = new ChainedParameterResolver(parameterResolver, new ResolvableParameterList(listParameters));
        final ResourceResolver resourceResolver = ResourceResolverHelper.resourceResolverFrom(resolver, new NullParameterResolver());
        final Injector injector = new SimpleInjector();
        injector.inject(cache, resourceResolver);
    }
}
