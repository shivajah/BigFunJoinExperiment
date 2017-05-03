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

import config.Constants;
import structure.Pair;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;


public abstract class AbstractReadOnlyClientUtility extends AbstractClientUtility {

    private final String qIndexFile;
    private final String qGenConfigFile;
    public ArrayList<Pair> qvids;
    protected boolean dumpResults = true;

    public abstract void executeQuery(int qid, int vid, String qBody);

    public AbstractReadOnlyClientUtility(String qIxFile, String qGenConfigFile, String statsFile, int ignore,
                                         String workloadFile, String resultsFile) {
        super(statsFile, resultsFile, ignore);
        this.qIndexFile = qIxFile;
        this.qGenConfigFile = qGenConfigFile;
        loadWorkloadFile(workloadFile);
    }

    private void loadWorkloadFile(String workloadFile) {
        if (qvids == null) {
            this.qvids =  new ArrayList<Pair>();
        }
        this.qvids.clear();
        try {
            BufferedReader in = new BufferedReader(new FileReader(workloadFile));
            String str;
            while ((str = in.readLine()) != null) {
                if (str.startsWith(Constants.COMMENT_TAG) || (str.length() == 0)) { //ignore comment lines
                    continue;
                }
                StringTokenizer st = new StringTokenizer(str, Constants.QSEQ_FILE_DELIM);
                int qIdentifier = Integer.parseInt(st.nextToken());
                int version = 0; //default version
                if (st.hasMoreTokens()) {
                    version = Integer.parseInt(st.nextToken());
                }
                qvids.add(new Pair(qIdentifier, version));
            }
            in.close();
        } catch (IOException e) {
            System.err.println("Error in loading queries sequence in AsterixClient for Random Workloads !");
            e.printStackTrace();
        }
    }

    protected void setDumpResults(boolean b) {
        this.dumpResults = b;
    }

    public String getQIxFile() {
        return qIndexFile;
    }

    public String getQGenConfigFile() {
        return qGenConfigFile;
    }
}