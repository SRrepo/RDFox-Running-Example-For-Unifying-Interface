package MainFiles;

import IO.FileServer;
import IO.Serialization.Configuration;
import IO.Serialization.TransferObject;
import RDFox.DatastoreBatchUpdater;
import RDFox.RDFoxWrapper;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.sun.org.slf4j.internal.Logger;
import com.sun.org.slf4j.internal.LoggerFactory;
import org.apache.commons.io.IOUtils;
import tech.oxfordsemantic.jrdfox.exceptions.JRDFoxException;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class RDFoxRunningExample {
    private static final Logger logger = LoggerFactory.getLogger(RDFoxRunningExample.class);
    static RDFoxWrapper rdFoxWrapper;
    public static List<TransferObject> answers = new ArrayList<>();
    public static boolean streamsRunning = true;

    public static void main(String[] args) throws IOException, InterruptedException, JRDFoxException {
        String rdfoxLicenseKey = getRDFoxLicenseKey(args);
        int queryInterval = getQueryInterval(args);
        Configuration config = new Gson().fromJson(IOUtils.toString(new URL("http://localhost:11111/configuration.json"), StandardCharsets.UTF_8), Configuration.class);
        rdFoxWrapper = RDFoxWrapper.getRDFoxWrapper(config.getQueries(), rdfoxLicenseKey, queryInterval * config.getQueries().size());
        new Thread(new DatastoreBatchUpdater(RDFoxWrapper.getRDFoxWrapper())).start();
        for(String queryId : config.getQueries().keySet()) {
            startRDFoxStreams(config.getQueries().get(queryId));
            registerRDFoxQuery(queryId, config.getQueries().get(queryId));
        }

        while (streamsRunning) {
            Thread.sleep(200);
        }
        writeAnswersToURL("http://localhost:11112/answers.json");
    }


    private static void registerRDFoxQuery(String queryId, String query) {
        if (!RDFoxWrapper.getRDFoxWrapper().registeredQueries.containsKey(queryId)) {
            RDFoxResultObserver rro = new RDFoxResultObserver(queryId, RDFoxWrapper.getRDFoxWrapper().getServerConnection());
            RDFoxWrapper.getRDFoxWrapper().rdFoxResultObserver = rro;
            RDFoxWrapper.getRDFoxWrapper().registerQuery(query, rro, new Random().nextInt());
            logger.debug("Registering result observer: " + queryId);
            RDFoxWrapper.getRDFoxWrapper().registeredQueries.put(queryId, rro);
        }
    }

    private static void startRDFoxStreams(String query) {
        for(String streamName : getStreamNamesFromQuery(query)) {
            RDFoxStreamReceiver rsr = new RDFoxStreamReceiver(streamName, getPortNumberFromURL(streamName));
            new Thread(rsr).start();
        }
        //Thread.yield();
    }

    private static void writeAnswersToURL(String url) {
        try (OutputStream out = new FileOutputStream("answers.json")){
            System.out.println("Answers online");
            out.write(new GsonBuilder().setPrettyPrinting().create().toJson(answers).getBytes(StandardCharsets.UTF_8));
            FileServer fileServer = new FileServer(getPortNumberFromURL(url), url, new Gson().toJson(answers));
            Thread.sleep(20000);
            System.out.println("Answers offline");
            fileServer.stop();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static Set<String> getStreamNamesFromQuery(String query) {
        System.out.println(query);
        Set<String> streamNames = new HashSet<>();
        for(String line : query.split("\n")) {
            if(line.matches("(.)*>(\\s)+\\[range(.)*")) {
                streamNames.add(line.split("> \\[range")[0].split("<")[1]);
                System.out.println("Stream found: " + line.split("> \\[range")[0].split("<")[1]);
            }
            if(line.matches("(.)*>(\\s)+\\[RANGE(.)*")) {
                streamNames.add(line.split("> \\[RANGE")[0].split("<")[1]);
                System.out.println("Stream found: " + line.split("> \\[RANGE")[0].split("<")[1]);
            }
        }
        return streamNames;
    }

    private static int getPortNumberFromURL(String url) {
        return Integer.parseInt(url.split(":")[2].split("/")[0]);
    }

    private static String getRDFoxLicenseKey(String args[]) {
        HashMap<String, String> parameters = new HashMap<>();
        for (String s : args) {
            parameters.put(s.split("=")[0], s.split("=")[1]);
        }
        if (parameters.containsKey("rdfoxLicense"))
            return parameters.get("rdfoxLicense");
        return "";
    }

    private static int getQueryInterval(String args[]) {
        HashMap<String, String> parameters = new HashMap<>();
        for (String s : args) {
            parameters.put(s.split("=")[0], s.split("=")[1]);
        }
        if (parameters.containsKey("queryInterval"))
            return Integer.parseInt(parameters.get("queryInterval"));
        return 15;
    }

}
