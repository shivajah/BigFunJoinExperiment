package asterixReadOnlyClient;

import structure.Pair;
import structure.Query;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by shiva on 2/2/17.
 */
public class AsterixMemoryAdjustedReadOnlyWorkload extends AsterixClientReadOnlyWorkload{
    int minJoinMemory;
    int joinMemoryDelta;
    int maxJoinMemory;
    int iteration;

    public AsterixMemoryAdjustedReadOnlyWorkload(String cc, String dvName, String qGenConfigFile, String qIxFile,
                                                 String statsFile, int ignore, String qSeqFile, String resDumpFile, long seed, long maxUsrId){
        super();
        this.ccUrl = cc;
        this.dvName = dvName;
        this.minJoinMemory = 1024; //4gb
        this.joinMemoryDelta = 512;
        this.maxJoinMemory = 6144; //13gb
        this.iteration = 3;
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
        String[] filenames = {"Hybrid.txt", "NBJ50.txt", "NBJ.txt"};
        BufferedWriter bw = null;
        FileWriter fw = null;
        File hybrid = new File("Hybrid.txt");
        File nbj = new File("NBJ.txt");
        File nbj50 = new File("NBJ50.txt");
        List<File> fileList = new LinkedList<>();
        fileList.add(hybrid);
        fileList.add(nbj);
        fileList.add(nbj50);
        for (File f : fileList) {
            try {
                if (f.exists()) {
                    f.delete();
                }
                f.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
        int i = 0;
        Query cacheCleaner = rwg.nextQuery(3015, 1);
        int joinMemory = maxJoinMemory;
        while (joinMemory >= minJoinMemory) {
            for (Pair qvPair : clUtil.qvids) {
                File f;
                if (i % 3 == 0) {
                    f = hybrid;
                } else if (i % 3 == 1) {
                    f = nbj50;
                } else {
                    f = nbj;
                }
                try {
                    fw = new FileWriter(f.getAbsoluteFile(), true);
                    bw = new BufferedWriter(fw);
                    int qid = qvPair.getQId();
                    int vid = qvPair.getVId();
                    Query q = rwg.nextQuery(qid, vid);
                    long startTime;
                    long endTime;
                    long diff = 0l;
                    if (q == null) {
                        continue; //do not break, if one query is not found
                    }
                    if (execQuery) {
                        clUtil.executeQuery(3015, 1, cacheCleaner.aqlPrint(dvName));//clean the cache
                        for (int it = 0; it < iteration; it++) {
                            startTime = System.currentTimeMillis();
                            clUtil.executeQuery(qid, vid, q.aqlPrint(dvName));
                            endTime = System.currentTimeMillis();
                            diff += endTime - startTime;
                        }
                        String result = joinMemory / 1024 + " " + diff / iteration + "\n";
                        bw.write(result);
                    }

            } catch(IOException e){
                e.printStackTrace();
            } finally{

                try {
                    if (bw != null)
                        bw.close();

                    if (fw != null)
                        fw.close();

                } catch (IOException ex) {

                    ex.printStackTrace();

                }
            }
            i++;
        }
        joinMemory = joinMemory - joinMemoryDelta;
    }
        clUtil.terminate();
    }

}
