package RDFox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.oxfordsemantic.jrdfox.Prefixes;
import tech.oxfordsemantic.jrdfox.client.DataStoreConnection;
import tech.oxfordsemantic.jrdfox.client.QueryAnswerMonitor;
import tech.oxfordsemantic.jrdfox.client.TransactionType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class NamedQuery implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(NamedQuery.class);

    public String query;
    public List<String> staticStreams;
    public List<NamedStream> streams;
    public int answerFrequencyInMilliSeconds;
    public QueryAnswerMonitor queryAnswerMonitor;
    public boolean stop = false;



    public NamedQuery (NamedQuery namedQuery) {
        this.query = namedQuery.query;
        this.staticStreams = new ArrayList<>(namedQuery.staticStreams);
        this.streams = new ArrayList<>();
        for(NamedStream stream : namedQuery.streams) {
            streams.add(new NamedStream(stream));
        }
        this.answerFrequencyInMilliSeconds = namedQuery.answerFrequencyInMilliSeconds;
        this.queryAnswerMonitor = null;
        this.stop = namedQuery.stop;
    }

    public NamedQuery(String query, List<String> staticStream, List<NamedStream> streams, int queryInterval) {
        this.query = query;
        this.staticStreams = staticStream;
        this.streams = streams;
        //this.answerFrequencyInMilliSeconds = streams.stream().mapToInt(c -> c.stepSizeInMilliSeconds).min().orElse(1000);
        this.answerFrequencyInMilliSeconds = queryInterval;
    }

    @Override
    public String toString() {
        return "NamedQuery{" +
                "query='" + query + '\'' +
                ", staticStreams=" + staticStreams +
                ", streams=" + streams +
                ", answerFrequencyInMilliSeconds=" + answerFrequencyInMilliSeconds +
                ", queryAnswerMonitor=" + queryAnswerMonitor +
                ", stop=" + stop +
                '}';
    }

    @Override
    public void run() {
        final long queryStartTime = System.currentTimeMillis();
        long i = 1;
        while (!stop) {
            waitUntil(queryStartTime + i * answerFrequencyInMilliSeconds);
            i++;
            if(!RDFoxWrapper.pause) {
                try (DataStoreConnection dataStoreConnection = RDFoxWrapper.getRDFoxWrapper().getServerConnection().newDataStoreConnection(RDFoxWrapper.datastoreName)) {
                    //dataStoreConnection.evaluateQuery(Prefixes.s_emptyPrefixes, query, new HashMap<>(), queryAnswerMonitor);
                    dataStoreConnection.beginTransaction(TransactionType.READ_ONLY);
                    dataStoreConnection.evaluateQuery("", Prefixes.s_emptyPrefixes, query, new HashMap<>(), queryAnswerMonitor);
                    dataStoreConnection.commitTransaction();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void waitUntil(long targetTime) {
        long millis = targetTime - System.currentTimeMillis();
        if (millis <= 0)
            return;
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        }
    }

    public void stop() {
        if (!stop) {
            stop = true;
            logger.info("Stopping query");
            for(NamedStream stream : this.streams) {
                stream.stop();
            }
        }
    }
}
