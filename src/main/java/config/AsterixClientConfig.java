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
package config;

import asterixReadOnlyClient.AsterixClientReadOnlyWorkload;
import asterixReadOnlyClient.AsterixMemoryAdjustedReadOnlyWorkload;
import asterixUpdateClient.AsterixClientUpdateWorkload;
import client.AbstractReadOnlyClient;
import client.AbstractUpdateClient;
import structure.UpdateTag;

public class AsterixClientConfig extends AbstractClientConfig {

    public AsterixClientConfig(String clientConfigFile) {
        super(clientConfigFile);
    }

    public void concurrentReadOnlyClientConfig(String bigFunHomePath) {

    }

    public AbstractReadOnlyClient readReadOnlyClientConfig(String bigFunHomePath) {
        String cc = "127.0.0.1";//(String) getParamValue(Constants.CC_URL);
        String dvName = (String) getParamValue(Constants.ASTX_DV_NAME);
        int iter = (int) getParamValue(Constants.ITERATIONS);

        String qIxFile = bigFunHomePath + "/files/" + Constants.Q_IX_FILE_NAME;
        String qGenConfigFile = bigFunHomePath + "/files/" + Constants.Q_GEN_CONFIG_FILE_NAME;
        String workloadFile = bigFunHomePath + "/files/" + Constants.WORKLOAD_FILE_NAME;

        String statsFile = bigFunHomePath + "/files/output/" + Constants.STATS_FILE_NAME;
        if (isParamSet(Constants.STATS_FILE)) {
            statsFile = (String) getParamValue(Constants.STATS_FILE);
        }

        long seed = Constants.DEFAULT_SEED;
        if (isParamSet(Constants.SEED)) {
            Object value = getParamValue(Constants.SEED);
            if (value instanceof Long) {
                seed = (long) value;
            } else if (value instanceof Integer) {
                seed = ((Integer) value).longValue();
            } else {
                System.err.println("WARNING: Invalid Seed value in " + Constants.BIGFUN_CONFIG_FILE_NAME
                        + " . Using default seed value for the generator.");
            }

        }

        long maxUserId = Constants.DEFAULT_MAX_GBOOK_USR_ID;
        if (isParamSet(Constants.MAX_GBOOK_USR_ID)) {
            Object value = getParamValue(Constants.MAX_GBOOK_USR_ID);
            if (value instanceof Long) {
                maxUserId = (long) value;
            } else if (value instanceof Integer) {
                maxUserId = ((Integer) value).longValue();
            } else {
                System.err.println("WARNING: Invalid " + Constants.MAX_GBOOK_USR_ID + " value in "
                        + Constants.BIGFUN_CONFIG_FILE_NAME + " . Using the default value for the generator.");
            }
        }

        int ignore = -1;
        if (isParamSet(Constants.IGNORE)) {
            ignore = (int) getParamValue(Constants.IGNORE);
        }

        boolean qExec = true;
        if (isParamSet(Constants.EXECUTE_QUERY)) {
            qExec = (boolean) getParamValue(Constants.EXECUTE_QUERY);
        }

        boolean dumpResults = false;
        String resultsFile = null;
        if (isParamSet(Constants.ASTX_DUMP_RESULTS)) {
            dumpResults = (boolean) getParamValue(Constants.ASTX_DUMP_RESULTS);
            resultsFile = (String) getParamValue(Constants.RESULTS_DUMP_FILE);
        }

        int numReaders = 1;
        if (isParamSet(Constants.NUM_CONCURRENT_READERS)) {
            numReaders = (int) getParamValue(Constants.NUM_CONCURRENT_READERS);
        }

        /* Memory Adjustment */

        int joinMemory = -1; int joinMemoryDelta = -1;
        if (isParamSet(Constants.JOIN_MEMORY)){
            joinMemory = (int) getParamValue(Constants.JOIN_MEMORY);
            joinMemoryDelta = isParamSet(Constants.JOIN_MEMORY_DELTA) ? (int) getParamValue(Constants.JOIN_MEMORY_DELTA):isParamSet(Constants.GENERAL_MEMORY_DELTA)?(int) getParamValue(Constants.GENERAL_MEMORY_DELTA):-1;
        }

        int groupMemory = -1; int groupMemoryDelta = -1;
        if (isParamSet(Constants.GROUP_MEMORY)){
            groupMemory = (int) getParamValue(Constants.GROUP_MEMORY);
            groupMemoryDelta = isParamSet(Constants.GROUP_MEMORY_DELTA) ? (int) getParamValue(Constants.GROUP_MEMORY_DELTA):isParamSet(Constants.GENERAL_MEMORY_DELTA)?(int) getParamValue(Constants.GENERAL_MEMORY_DELTA):-1;
        }

        int frameSize = -1;
        if(isParamSet(Constants.FRAMESIZE)) {
            frameSize = (int) getParamValue(Constants.FRAMESIZE);
        }
        AsterixClientReadOnlyWorkload rClient;
        //if (numReaders == 1) {
         //   if(frameSize > 0 && (joinMemory > 0 || groupMemory > 0)){
                rClient = new AsterixMemoryAdjustedReadOnlyWorkload(cc, dvName,qGenConfigFile,qIxFile,statsFile,ignore,
                        workloadFile,statsFile,seed,maxUserId);
//            }
//            else {
//                rClient = getAsterixClientReadOnlyWorkload(cc, dvName, iter, qIxFile, qGenConfigFile, workloadFile, statsFile, seed, maxUserId, ignore, resultsFile);
//            }
//        }
//        else {
//            rClient = new AsterixConcurrentReadOnlyWorkload(cc, dvName, iter, qGenConfigFile,
//                    qIxFile, statsFile, ignore, workloadFile, /*dumpDirFile,*/ resultsFile, seed, maxUserId, numReaders);
//        }



        rClient.setExecQuery(qExec);
        //TODO: This needs to be set for every client in the concurrent workload.
        //rClient.setDumpResults(dumpResults);
        return rClient;
    }

    private AsterixClientReadOnlyWorkload getAsterixClientReadOnlyWorkload(String cc, String dvName, int iter, String qIxFile, String qGenConfigFile, String workloadFile, String statsFile, long seed, long maxUserId, int ignore, String resultsFile) {
        return new AsterixClientReadOnlyWorkload(cc, dvName, iter, qGenConfigFile,
                    qIxFile, statsFile, ignore, workloadFile, /*dumpDirFile,*/ resultsFile, seed, maxUserId);
    }

    @Override
    public AbstractUpdateClient readUpdateClientConfig(String bigFunHomePath, String updateType) {
        String cc = (String) getParamValue(Constants.CC_URL);
        String oprType = (String) getParamValue(Constants.UPDATE_OPR_TYPE_TAG);

        String updatesFile = (String) getParamValue(Constants.UPDATES_FILE);
        String statsFile = bigFunHomePath + "/files/output/" + Constants.STATS_FILE_NAME;
        if (isParamSet(Constants.STATS_FILE)) {
            statsFile = (String) getParamValue(Constants.STATS_FILE);
        }

        String dvName = (String) getParamValue(Constants.ASTX_DV_NAME);
        String dsName = (String) getParamValue(Constants.ASTX_DS_NAME);
        String keyName = (String) getParamValue(Constants.ASTX_KEY_NAME);
        int batchSize = (int) getParamValue(Constants.UPDATE_BATCH_SIZE);

        int limit = -1;
        if (isParamSet(Constants.UPDATE_LIMIT)) {
            limit = (int) getParamValue(Constants.UPDATE_LIMIT);
        }

        int ignore = -1;
        if (isParamSet(Constants.IGNORE)) {
            ignore = (int) getParamValue(Constants.IGNORE);
        }

        int threadPoolSize = Constants.DEFAULT_THREAD_POOL_SIZE;
        if (isParamSet(Constants.THREAD_POOL_SIZE)) {
            threadPoolSize = (int) getParamValue(Constants.THREAD_POOL_SIZE);
        }

        UpdateTag upTag = null;
        if (oprType.equals(Constants.INSERT_OPR_TYPE)) {
            upTag = UpdateTag.INSERT;
        } else if (oprType.equals(Constants.DELETE_OPR_TYPE)) {
            upTag = UpdateTag.DELETE;
        } else {
            System.err.println("Unknow Data Manipulation Operation for AsterixDB - " + oprType);
            System.err.println("You can only run " + Constants.INSERT_OPR_TYPE + " and " + Constants.DELETE_OPR_TYPE
                    + " against AsterixDB");
            return null;
        }
        switch (updateType) {
            case Constants.ASTX_CONCURRENT_UPDATE_CLIENT_TAG:
                return new AsterixClientUpdateWorkload(cc, dvName, dsName, keyName, upTag, batchSize, limit, updatesFile,
                        statsFile, ignore, threadPoolSize);
            case Constants.ASTX_UPDATE_CLIENT_TAG:
            default:
                return new AsterixClientUpdateWorkload(cc, dvName, dsName, keyName, upTag, batchSize, limit, updatesFile,
                        statsFile, ignore);
        }
    }

    public AbstractUpdateClient readHybridWorkloadConfiguration(String bigFunHomePath) {
        return null;
    }
}
