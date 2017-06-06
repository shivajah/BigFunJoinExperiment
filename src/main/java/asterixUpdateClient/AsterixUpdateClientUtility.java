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
package asterixUpdateClient;

import client.AbstractUpdateClientUtility;
import config.Constants;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import structure.AqlUpdate;
import structure.Update;
import workloadGenerator.AbstractUpdateWorkloadGenerator;

import java.time.Duration;
import java.time.Instant;

public class AsterixUpdateClientUtility extends AbstractUpdateClientUtility {

    final int TRACE_PACE = 500;

    String ccUrl;
    //DefaultHttpClient httpclient;
    CloseableHttpClient httpclient;
    //HttpPost httpPost;
    volatile int counter = 0; //for trace only

    public static volatile int failedTxns = 0;
    public static volatile int passedTxns = 0;
    public Instant startTime;


    public AsterixUpdateClientUtility(String cc, int batchSize, int limit, AbstractUpdateWorkloadGenerator uwg,
                                      String updatesFile, String statsFile, int ignore) {
        super(batchSize, limit, uwg, updatesFile, statsFile, ignore);
        this.ccUrl = cc;
    }

    @Override
    public void init() {
        //httpclient = new DefaultHttpClient();
        httpclient = createPooledHTTPConnections();
        startTime = Instant.now();
        //httpPost = new HttpPost(getUpdateUrl());
    }

    @Override
    public void terminate() {
        httpclient.getConnectionManager().shutdown();
    }

    // TODO: Make this multi-threaded.
    @Override
    public void executeUpdate(int qid, Update update) {
        //DefaultHttpClient httpclient;
        //HttpPost httpPost;
        //httpclient = new DefaultHttpClient();
        HttpPost httpPost = new HttpPost(getUpdateUrl());
        //init();
        //System.out.println("Execute update called in thread: " + Thread.currentThread().getName());
        long rspTime = Constants.INVALID_TIME;
        String updateBody = null;
        HttpResponse response;
        try {
            updateBody = ((AqlUpdate) update).printAqlStatement();
            httpPost.setEntity(new StringEntity(updateBody));

            long s = System.currentTimeMillis();
            response = httpclient.execute(httpPost);
            HttpEntity entity = response.getEntity();
            EntityUtils.consume(entity);
            long e = System.currentTimeMillis();
            rspTime = (e - s);
        } catch (Exception e) {
            System.err.println("Problem in running update " + qid + " against Asterixdb ! " + e.getMessage());
            updateStat(qid, 0, Constants.INVALID_TIME);
            return;
        }


        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode != HttpStatus.SC_OK) {
            System.err.println("Update " + qid + " against Asterixdb returned http error code : " + statusCode + "Query: " + updateBody + " Thread: " + Thread.currentThread().getName());
            rspTime = Constants.INVALID_TIME;
            failedTxns++;
        }
        else {
            passedTxns++;
            if (passedTxns % 100 == 0) {
                System.out.println("QPS: " + passedTxns / (1.0 * Duration.between(startTime, Instant.now()).getSeconds()));
            }
        }
        updateStat(qid, 0, rspTime);

        if (++counter % TRACE_PACE == 0) {
            System.out
                    .println(counter + " Updates done - last one took\t" + rspTime + " ms\tStatus-Code\t" + statusCode);
        }

    }

    private String getUpdateUrl() {
       // return ("http://" + ccUrl + ":" + Constants.ASTX_AQL_REST_API_PORT + "/update");
        return ("http://sensorium-38.ics.uci.edu:" + Constants.ASTX_AQL_REST_API_PORT + "/update");
    }

    @Override
    public void resetTraceCounters() {
        counter = 0;
    }

    public CloseableHttpClient createPooledHTTPConnections() {

        PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
// Increase max total connection to 200
        cm.setMaxTotal(200);
// Increase default max connection per route to 20
        //cm.setDefaultMaxPerRoute(20);
// Increase max connections for localhost:80 to 50
        /*HttpHost localhost = new HttpHost("locahost", 80);
        cm.setMaxPerRoute(new HttpRoute(localhost), 50);*/

        CloseableHttpClient httpClient = HttpClients.custom()
                .setConnectionManager(cm)
                .build();
        return  httpClient;

    }
}
