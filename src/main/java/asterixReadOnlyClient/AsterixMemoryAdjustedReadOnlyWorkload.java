package asterixReadOnlyClient;
import structure.Pair;
import structure.Query;

import java.io.*;

/**
 * Created by shiva on 2/2/17.
 */
public class AsterixMemoryAdjustedReadOnlyWorkload extends AsterixClientReadOnlyWorkload{
    int minJoinMemory;
    int joinMemoryDelta;
    int maxJoinMemory;
    double frameSize =-1;
    double groupMemory=-1;

    public AsterixMemoryAdjustedReadOnlyWorkload(String cc, String dvName, String qGenConfigFile, String qIxFile,
                                                 String statsFile, int ignore, String qSeqFile, String resDumpFile, long seed, long maxUsrId){
        super();
        this.ccUrl = cc;
        this.dvName = dvName;
        this.minJoinMemory = 4096; //4gb
        this.joinMemoryDelta = 1024;
        this.maxJoinMemory = 13312; //13gb
        setClientUtil(qIxFile, qGenConfigFile, statsFile, ignore, qSeqFile, resDumpFile);
        clUtil.init();
        initReadOnlyWorkloadGen(seed, maxUsrId);
        execQuery = true;

    }
    @Override
    public void execute() {
//        String[] filenames = {"NBJ.txt","Hybrid.txt"};
//        BufferedWriter bw = null;
//        FileWriter fw = null;
//        for (int i = 0; i<filenames.length;
//             i++) {
//            File f = new File(filenames[i]);
//            try{
//                if (!f.exists()) {
//                    f.createNewFile();
//                }
//                fw = new FileWriter(f.getAbsoluteFile(), true);
//                bw = new BufferedWriter(fw);
//                double joinMemory = minJoinMemory;
//                int qid = clUtil.qvids.get(1).getQId();
//                int vid = clUtil.qvids.get(1).getVId();
//                Query q = rwg.nextQuery(qid, vid);
//                long duration = 0 ;
//                while (joinMemory <= maxJoinMemory) {
//                    long startTime = System.currentTimeMillis();
//                    if (execQuery) {
//                        clUtil.executeQuery(qid, vid, q.aqlPrint(dvName, joinMemory, groupMemory, frameSize));
//                    }
//                    long endTime = System.currentTimeMillis();
//                     duration = (endTime -  startTime);
//                    joinMemory += joinMemoryDelta;
//                }
//                String result = joinMemory  + ' ' + Long.toString(duration);
//                bw.write(result);
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//            finally {
//
//                try {
//                    clUtil.terminate();
//                    if (bw != null)
//                        bw.close();
//
//                    if (fw != null)
//                        fw.close();
//
//                } catch (IOException ex) {
//
//                    ex.printStackTrace();
//
//                }
//            }
//
//
//        }
        String[] filenames = {"NBJ.txt","Hybrid.txt","NBJ50.txt"};
        BufferedWriter bw = null;
        FileWriter fw = null;
        int i = 0;
        for (Pair qvPair : clUtil.qvids) {
            File f = new File(filenames[i]);
            try {
                if (!f.exists()) {
                    f.createNewFile();
                } else {
                    f.delete();
                    f.createNewFile();
                }
                fw = new FileWriter(f.getAbsoluteFile(), true);
                bw = new BufferedWriter(fw);
                int qid = qvPair.getQId();
                int vid = qvPair.getVId();
                Query q = rwg.nextQuery(qid, vid);
                int joinMemory = minJoinMemory;
                while (joinMemory <= maxJoinMemory) {
                    long startTime = 0l;
                    long endTime = 0l;
                    if (q == null) {
                        continue; //do not break, if one query is not found
                    }
                    if (execQuery) {
                        startTime = System.currentTimeMillis();
                        clUtil.executeQuery(qid, vid, q.aqlPrint(dvName));
                        endTime = System.currentTimeMillis();
                        String result = joinMemory/1024 + " " + (endTime - startTime )+"\n";
                        bw.write(result);
                    }
                    joinMemory += joinMemoryDelta;
                }
                i++;
            } catch (IOException e) {
                e.printStackTrace();
            } finally {

                try {
                    if (bw != null)
                        bw.close();

                    if (fw != null)
                        fw.close();

                } catch (IOException ex) {

                    ex.printStackTrace();

                }
            }

        }
        clUtil.terminate();
    }

}
