package asterixReadOnlyClient;

import client.AbstractReadOnlyClientUtility;
import structure.Pair;
import structure.Query;
import workloadGenerator.ReadOnlyWorkloadGenerator;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

/**
 * Created by msa on 5/1/16.
 */
public class AsterixConcurrentReadOnlyWorkload extends AsterixClientReadOnlyWorkload {

    private ExecutorService executorService;

    private Map<Integer, ReadOnlyWorkloadGenerator> rwgMap;

    private Map<Integer, AbstractReadOnlyClientUtility> clUtilMap;

    private int numReaders;

    private List<Long> readerSeeds;

    public AsterixConcurrentReadOnlyWorkload(String cc, String dvName, int iter, String qGenConfigFile, String
            qIxFile, String statsFile, int ignore, String qSeqFile, String resDumpFile, long seed, long maxUsrId,
                                             int numReaders) {
        super();
        this.ccUrl = cc;
        this.dvName = dvName;
        this.iterations = iter;
        this.numReaders = numReaders;
        clUtilMap = new HashMap<>();
        initReaderSeeds(seed);
        setClientUtil(qIxFile, qGenConfigFile, statsFile, ignore, qSeqFile, resDumpFile);
        initReadOnlyWorkloadGen(seed, maxUsrId);
        initExecutors();
        execQuery = true;
        //super(cc, dvName, iter, qGenConfigFile, qIxFile, statsFile, ignore, qSeqFile, resDumpFile, seed, maxUsrId);
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
            executorService.shutdownNow();
            System.out.println("Shutdown complete!");
        }
    }

    private void initReaderSeeds(long seed) {
        readerSeeds = new ArrayList<>();
        Random rand = new Random(seed);
        IntStream.range(0, numReaders).forEach(x -> {
            readerSeeds.add(rand.nextLong());
        });
    }

    private void initExecutors() {
        executorService = Executors.newFixedThreadPool(this.numReaders);
    }

    @Override
    public void initReadOnlyWorkloadGen(long seed, long maxUsrId) {
        rwgMap = new HashMap<>();
        IntStream.range(0, numReaders).forEach(x -> {
            rwgMap.put(x, new ReadOnlyWorkloadGenerator(clUtilMap.get(x).getQIxFile(), clUtilMap.get(x)
                    .getQGenConfigFile(), readerSeeds.get(x), maxUsrId));
        });
    }

    @Override
    public void setClientUtil(String qIxFile, String qGenConfigFile, String statsFile, int ignore,
                                          String qSeqFile, String resultsFile) {
        //TODO: Append the result and other stat files with threadIds.
        IntStream.range(0, numReaders).forEach(x -> {
            clUtilMap.put(x, new AsterixReadOnlyClientUtility(ccUrl, qIxFile, qGenConfigFile, statsFile, ignore, qSeqFile,
                    resultsFile));
            clUtilMap.get(x).init();
        });
    }

    @Override
    public void execute() {
        IntStream.range(0, numReaders).forEach(readerId -> {
            executorService.submit(() -> {
                long iteration_start = 0l;
                long iteration_end = 0l;
                for (int i = 0; i < iterations; i++) {
                    System.out.println("\nAsterixDB Client - Read-Only Workload - Starting Iteration " + i + " in " +
                            "thread: " + readerId + " (" + Thread.currentThread().getName() + ")");
                    iteration_start = System.currentTimeMillis();
                    for (Pair qvPair : clUtilMap.get(readerId).qvids) {
                        int qid = qvPair.getQId();
                        int vid = qvPair.getVId();
                        Query q = rwgMap.get(readerId).nextQuery(qid, vid);
                        if (q == null) {
                            continue; //do not break, if one query is not found
                        }
                        if (execQuery) {
                            clUtilMap.get(readerId).executeQuery(qid, vid, q.aqlPrint(dvName));
                        }
                    }
                    iteration_end = System.currentTimeMillis();
                    System.out.println("Total time for iteration " + i + " :\t" + (iteration_end - iteration_start) +
                            " ms in thread: " + readerId + " (" + Thread.currentThread().getName() + ")");
                }
                clUtilMap.get(readerId).terminate();
            });
        });
        shutDownExecutors();
    }

    @Override
    public void setDumpResults(boolean b) {
        IntStream.range(0, numReaders).forEach(x -> {

        });
    }

    @Override
    public void generateReport() {
        IntStream.range(0, numReaders).forEach(x -> clUtilMap.get(x).generateReport());
    }
}
