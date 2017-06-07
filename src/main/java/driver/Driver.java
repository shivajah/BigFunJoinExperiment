/*
 * Copyright by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package driver;

import client.AbstractClient;
import config.AbstractClientConfig;
import config.AsterixClientConfig;
import config.Constants;

import java.time.Instant;

public class Driver {
    public static void main(String[] args) {

        if (args.length != 1) {
            System.out.println("Correct Usage:\n");
            System.out.println("\t[0]: BigFUN home has to be set to a valid path.");
            return;
        }

        String bigFunHome = args[0];
        bigFunHome = bigFunHome.replaceAll("/$", "");
        String clientConfigFile = bigFunHome + "/conf/bigfun-conf.json";
        AbstractClientConfig clientConfig = new AsterixClientConfig(clientConfigFile);
        clientConfig.parseConfigFile();
        if (!clientConfig.isParamSet(Constants.CLIENT_TYPE)) {
            System.err.println("The Client Type is not set to a valid value in the config file.");
            return;
        }
        String clientTypeTag = (String) clientConfig.getParamValue(Constants.CLIENT_TYPE);
        AbstractClient client = null;
        switch (clientTypeTag) {
            case Constants.ASTX_MEMORY_ADJUSTED_READONLY_CLIENT_TAG:
            case Constants.ASTX_RANDOM_CLIENT_TAG:
                client = clientConfig.readReadOnlyClientConfig(bigFunHome);
                break;
            case Constants.ASTX_UPDATE_CLIENT_TAG:
            case Constants.ASTX_CONCURRENT_UPDATE_CLIENT_TAG:
                client = clientConfig.readUpdateClientConfig(bigFunHome, clientTypeTag);
                break;

                //client =  TODO
            default:
                System.err.println("Unknown/Invalid client type:\t" + clientTypeTag);
        }
        Instant current, end;
        current = Instant.now();
        client.execute();
        end = Instant.now();
        //System.out.println("Total time: " + Duration.between(end, current));
     //   System.out.println("Total Passed: " + AsterixUpdateClientUtility.passedTxns);
      //  System.out.println("Total failed: " + AsterixUpdateClientUtility.failedTxns);
        //TODO: in case of concurrent workloads, call the right client workload.
        // client.generateReport();
        System.out.println("\nBigFUN Benchmark is done.\n");
    }

}
