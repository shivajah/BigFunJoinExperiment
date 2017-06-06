package asterixUpdateClient;

import client.AbstractUpdateClient;
import client.AbstractUpdateClientUtility;
import config.Constants;
import org.apache.commons.math3.distribution.AbstractRealDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.PoissonDistribution;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import structure.AqlUpdate;
import structure.Update;
import workloadGenerator.AbstractUpdateWorkloadGenerator;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

/**
 * Created by msa on 4/24/16.
 */
public class AsterixConcurrentUpdateWorkload extends AbstractUpdateClientUtility {

    private final int TRACE_PACE = 500;

    private String ccUrl;

    private PoolingHttpClientConnectionManager cm;

    private CloseableHttpClient httpclient;

    private Map<String, List<Long>> timeCounters;

    //private AbstractRealDistribution waitTimeDistribution;
    private PoissonDistribution waitTimeDistribution;

    private final double DISTRIBUTION_MEAN = 10;

    private final double DISTRIBUTION_EPSILON = 10;

    private boolean NO_WAIT = true;

    private volatile int counter = 0;

    private ExecutorService executorService;

    private int threadPoolSize;

    public AsterixConcurrentUpdateWorkload(String cc, int batchSize, int limit, AbstractUpdateWorkloadGenerator uwg,
                                      String updatesFile, String statsFile, int ignore, int threadPoolSize) {
        super(batchSize, limit, uwg, updatesFile, statsFile, ignore);
        this.ccUrl = cc;
        timeCounters = new ConcurrentHashMap<>();
        this.threadPoolSize = threadPoolSize;
        waitTimeDistribution = new PoissonDistribution(DISTRIBUTION_MEAN, DISTRIBUTION_EPSILON);
        //waitTimeDistribution = new NormalDistribution();
        //executorService = Executors.newFixedThreadPool(threadPoolSize);

    }

    private synchronized long getWaitTime() {
        if (NO_WAIT) {
            return 0;
        }
        else {
            return waitTimeDistribution.sample();
        }
    }

    @Override
    protected void executeUpdate(int qid, Update update) {
        HttpPost httpPost = new HttpPost(getUpdateUrl());

        String updateBody = null;
        HttpResponse response;
        try {
            updateBody = ((AqlUpdate) update).printAqlStatement();
            httpPost.setEntity(new StringEntity(updateBody));
            response = httpclient.execute(httpPost);
            HttpEntity entity = response.getEntity();
            EntityUtils.consume(entity);
            String threadName = Thread.currentThread().getName();
            long waitTime = getWaitTime();
            if (timeCounters.get(threadName) != null) {
                timeCounters.get(threadName).add(waitTime);
            } else {
                timeCounters.put(threadName, new ArrayList<Long>());
                timeCounters.get(threadName).add(waitTime);
            }
            counter++;
        } catch (Exception e) {
            System.err.println("Problem in running update " + qid + " against Asterixdb ! " + e.getMessage());
            System.out.println(((AqlUpdate) update).printAqlStatement());
            //failedTxns++;
            updateStat(qid, 0, Constants.INVALID_TIME);
            return;
        }

        if (counter % TRACE_PACE == 0) {
            printTimeCounters();
            System.out.println("Completed: " + counter);
        }

        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode != HttpStatus.SC_OK) {
            System.err.println("Update " + qid + " against Asterixdb returned http error code : " + statusCode + "Query: " + updateBody + " Thread: " + Thread.currentThread().getName());

        }
        updateStat(qid, 0, 0); //rspTime);
    }

    @Override
    public void printTimeCounters() {
        for (Map.Entry<String, List<Long>> entry : timeCounters.entrySet()) {
            System.out.print("Thread Name: " + entry.getKey());
            OptionalLong min = entry.getValue().stream().mapToLong(x -> x).min();
            OptionalLong max = entry.getValue().stream().mapToLong(x -> x).max();
            OptionalDouble avg = entry.getValue().stream().mapToLong(x -> x).average();
            System.out.print(" MIN: " + min.getAsLong());
            System.out.print(" MAX: " + max.getAsLong());
            System.out.println(" AVG: " + avg.getAsDouble());
        }
    }




    @Override
    protected void updateStat(int qid, int vid, long rspTime) {

    }

    @Override
    public void resetTraceCounters() {

    }

    @Override
    public void init() {
        httpclient = HttpClients.custom().setConnectionManager(cm).build();
    }

    @Override
    public void terminate() {

    }

    private String getUpdateUrl() {
        return ("http://sensorium-38.ics.uci.edu:" + Constants.ASTX_AQL_REST_API_PORT + "/update");
    }

}
