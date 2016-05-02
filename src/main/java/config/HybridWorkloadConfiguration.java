package config;

import client.AbstractReadOnlyClient;
import client.AbstractUpdateClient;

/**
 * Created by msa on 4/28/16.
 */
public class HybridWorkloadConfiguration extends AbstractClientConfig {

    public HybridWorkloadConfiguration(String clientConfigFile) {
        super(clientConfigFile);
    }

    @Override
    public AbstractReadOnlyClient readReadOnlyClientConfig(String bigFunHomePath) {
        return null;
    }

    @Override
    public AbstractUpdateClient readUpdateClientConfig(String bigFunHomePath, String updateType) {
        return null;
    }
}
