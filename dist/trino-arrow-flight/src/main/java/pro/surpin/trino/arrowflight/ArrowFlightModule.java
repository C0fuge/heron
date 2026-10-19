package pro.surpin.trino.arrowflight;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.trino.spi.type.TypeManager;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class ArrowFlightModule
        implements Module
{
    private final TypeManager typeManager;

    public ArrowFlightModule(TypeManager typeManager)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(TypeManager.class).toInstance(typeManager);

        configBinder(binder).bindConfig(ArrowFlightConfig.class);

        binder.bind(ArrowTypeConverter.class).in(Scopes.SINGLETON);
        binder.bind(ArrowFlightClient.class).in(Scopes.SINGLETON);
        binder.bind(ArrowFlightMetadata.class).in(Scopes.SINGLETON);
        binder.bind(ArrowFlightSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ArrowFlightPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(ArrowFlightConnector.class).in(Scopes.SINGLETON);
    }
}
