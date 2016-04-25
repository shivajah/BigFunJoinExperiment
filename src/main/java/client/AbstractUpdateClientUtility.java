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
package client;

import structure.AqlUpdate;
import structure.Update;
import workloadGenerator.AbstractUpdateWorkloadGenerator;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.Reader;
import java.io.StringReader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public abstract class AbstractUpdateClientUtility extends AbstractClientUtility {

    int limit; //how many batches to run (in case of early termination request) - if negative it processes all the workload file content.
    int batchSize;
    protected String updatesFile;
    AbstractUpdateWorkloadGenerator uwg;

    ExecutorService executorService;
    // TODO: Take this as param in constructor
    private final int DEFAULT_THREAD_POOL_SIZE = 10;

    public AbstractUpdateClientUtility(int batchSize, int limit, AbstractUpdateWorkloadGenerator uwg,
                                       String updatesFile, String statsFile, int ignore) {
        super(statsFile, null, ignore);
        this.batchSize = batchSize;
        this.limit = limit;
        this.updatesFile = updatesFile;
        this.uwg = uwg;
        executorService = Executors.newFixedThreadPool(DEFAULT_THREAD_POOL_SIZE);
    }

    protected abstract void executeUpdate(int qid, Update update);

    protected void updateStat(int qid, int vid, long rspTime) {
        sc.updateStat(qid, vid, rspTime);
    }

    public void generateReport() {
        sc.report();
    }

    public void processUpdates(int qid, boolean isWarmup) {
        try {
            BufferedReader in = new BufferedReader(new FileReader(updatesFile));
            int totalBatchCounter = 0;
            int currentBatchSize = 0;
            StringBuffer sb = null;
            String str;
            while ((limit < 0 || totalBatchCounter < limit) && ((str = in.readLine()) != null)) {
                if (sb == null) {
                    sb = new StringBuffer();
                }
                sb.append(str).append("\n");
                if ((++currentBatchSize) == batchSize) { //we have read enough to form the next batch
                    StringReader sr = new StringReader(sb.toString());
                    //runOneBatch(qid, sr, isWarmup);
                    dispatchUpdate(qid, sr, isWarmup);
                    sb = null;
                    currentBatchSize = 0;
                    totalBatchCounter++;
                }
            }

            if (currentBatchSize > 0) { //Last set of updates, whose size is less than batch size
                StringReader sr = new StringReader(sb.toString());
                //runOneBatch(qid, sr, isWarmup);
                dispatchUpdate(qid, sr, isWarmup);
            }

            in.close();
            shutDownExecutors();
        } catch (Exception e) {
            System.err.println("Problem in processing updates in Update Client Utility");
            e.printStackTrace();
        }
    }

    private void dispatchUpdate(int qid, Reader r, boolean isWarmup) {
        uwg.resetUpdatesInput(r);
        Update nextUpdate = uwg.getNextUpdate();
//        System.out.println(((AqlUpdate) nextUpdate).printAqlStatement());
        executorService.submit(() -> {
            //System.out.println("Executing update from thread " + Thread.currentThread().getName());
            executeUpdate(qid, nextUpdate);
        });
    }

    private void shutDownExecutors() {
        try {
            executorService.shutdown();
            executorService.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS); // TODO: Is this necessary?

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        finally {
            if (!executorService.isTerminated()) {
                System.out.println("canceling all pending tasks");
            }
            printTimeCounters();
            executorService.shutdownNow();
            System.out.println("Shutdown complete!");
        }
    }

    private void runOneBatch(int qid, Reader r, boolean isWarmup) {
        uwg.resetUpdatesInput(r);
        Update nextUpdate = uwg.getNextUpdate();
        executeUpdate(qid, nextUpdate);
    }

    public abstract void resetTraceCounters();

    public void printTimeCounters() {}
}
