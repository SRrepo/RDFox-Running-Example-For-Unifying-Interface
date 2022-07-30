package RDFox;

import MainFiles.RDFoxResultObserver;
import com.hp.hpl.jena.rdf.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.oxfordsemantic.jrdfox.Prefixes;
import tech.oxfordsemantic.jrdfox.client.*;
import tech.oxfordsemantic.jrdfox.exceptions.JRDFoxException;

import java.io.*;
import java.net.URL;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class RDFoxWrapper {
    private static final Logger logger = LoggerFactory.getLogger(RDFoxWrapper.class);

    public static final Property timestampProperty = ModelFactory.createDefaultModel().createProperty("http://example.de#hasTimestamp");

    private static RDFoxWrapper rdfoxWrapper;

    private ServerConnection serverConnection;

    private Map<String, NamedQuery> queries;

    private Set<String> staticData;

    public RDFoxResultObserver rdFoxResultObserver;

    public static String datastoreName = "Datastore";
    private int datastoreCounter = 1;
    public static boolean pause = false;

    private String queryId;
    private NamedQuery firstQuery;

    private static Map<Statement, Integer> statementsToBeDeleted = new ConcurrentHashMap();
    private static Map<Statement, Long> statementsToBeAdded = new ConcurrentHashMap();

    public ConcurrentHashMap<String, Object> registeredQueries = new ConcurrentHashMap<String, Object>();

    private int counterMethodCalled = 0;
    private int counterUpdateCalled = 0;

    private int counterAddition = 0;
    private int counterDeletion = 0;

    private int queryInterval = 0;

    public static boolean start = false;


    public static RDFoxWrapper getRDFoxWrapper() {
        if (rdfoxWrapper == null) {
            try {
                throw new Exception("RDFoxWrapper not initialised");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return rdfoxWrapper;
    }

    public static RDFoxWrapper getRDFoxWrapper(Map<String, String> queryMap, String rdfoxLicenseKey, int queryInterval) throws JRDFoxException {
        if (rdfoxWrapper == null) {
            rdfoxWrapper = new RDFoxWrapper(queryMap, rdfoxLicenseKey, queryInterval);
        }
        return rdfoxWrapper;
    }

    private RDFoxWrapper(Map<String, String> queryMap, String rdfoxLicenseKey, int queryInterval) throws JRDFoxException {
        final String serverURL = "rdfox:local";
        final String roleName = "nathan";
        final String password = "password";

        //RDFox Connection
        Map<String, String> parametersServer = new HashMap<String, String>();
        parametersServer.put("license-file", rdfoxLicenseKey);
        logger.debug(Arrays.toString(ConnectionFactory.startLocalServer(parametersServer)));
        ConnectionFactory.createFirstLocalServerRole(roleName, password);
        serverConnection = ConnectionFactory.newServerConnection(serverURL, roleName, password);
        //Datastore
        initializeDatastore();
        this.staticData = new HashSet<>();
        //Load Queries
        queries = new HashMap<>();
        this.queryInterval = queryInterval;
        try {
            loadStaticData();
        } catch (Exception e) {
            logger.error(e.toString());
        }
    }

    private NamedQuery parseNamedQuery(String query) {
        List<NamedStream> namedStreams = new ArrayList<>();
        List<String> staticStreams = new ArrayList<>();
        String newQuery = "";
        String beginning;
        try (Scanner scanner = new Scanner(query)) {
            beginning = scanner.nextLine();
            while (beginning.startsWith("FROM ")) {
                if (beginning.startsWith("FROM static")) {
                    staticStreams.add(beginning.split(" ")[2].substring(1, beginning.split(" ")[2].length() - 1));
                    staticData.add(beginning.split(" ")[2].substring(1, beginning.split(" ")[2].length() - 1));
                }
                if (beginning.startsWith("FROM dynamic")) {
                    String[] split = beginning.split(" ");
                    namedStreams.add(new NamedStream(split[3].substring(1, split[3].length() - 1), Integer.parseInt(split[5].substring(0, split[5].length() - 2)), Integer.parseInt(split[7].substring(0, split[7].length() - 3)), serverConnection));
                }
                beginning = scanner.nextLine();
            }
            while (scanner.hasNextLine()) {
                newQuery += beginning + "\n";
                beginning = scanner.nextLine();
            }
        }

        if (queryInterval == 0) {
            queryInterval = namedStreams.stream().mapToInt(c -> c.stepSizeInMilliSeconds).min().orElse(1000);
        }

        newQuery += beginning;
        return new NamedQuery(newQuery, staticStreams, namedStreams, queryInterval);
    }

    public void initializeDatastore() throws JRDFoxException {
        //serverConnection.createDataStore(RDFoxWrapper.datastoreName, "par-simple-nn", new HashMap<>());
        serverConnection.createDataStore(RDFoxWrapper.datastoreName, new HashMap<>());
    }

    public void loadStaticData() {
        for (String url : staticData) {
            try (DataStoreConnection dataStoreConnection = serverConnection.newDataStoreConnection(RDFoxWrapper.datastoreName)) {
                ImportResult importResult = dataStoreConnection.importData(UpdateType.ADDITION, Prefixes.s_emptyPrefixes, new BufferedInputStream(new URL(url).openStream()));
                logger.info("Static data imported: " + (importResult.getNumberOfChangedFacts() + importResult.getNumberOfChangedAxioms()));
            } catch (JRDFoxException | IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void putData(String uri, Statement statement) {
        //System.out.println("URI: " + uri);
        for (String s : queries.keySet()) {
            //System.out.println("Query: " + s);
            //queries.get(s).streams.forEach(c -> System.out.println("URI von Stream: " + c.uri));
            queries.get(s).streams.stream().filter(c -> c.uri.equals(uri)).forEach(c -> c.put(statement));
        }
    }

    public void flushIfNecessary(String uri) {
        for (String s : queries.keySet()) {
            queries.get(s).streams.stream().filter(c -> c.uri.equals(uri)).forEach(c -> c.flushIfNecessary());
        }
    }

    public void sendStartSignal(String uri) {
        for (String s : queries.keySet()) {
            queries.get(s).streams.stream().filter(c -> c.uri.equals(uri)).forEach(c -> c.sendStartSignal());
        }
    }


    public static void maintainStreamDatastore(Queue<ReifiedStatement> currentWindowStatements, List<ReifiedStatement> newTriples, long goOutTime, ServerConnection serverConnection, Prefixes prefixes) throws JRDFoxException {
        //System.out.println("Beginn: " + currentWindowTripels.size());
        currentWindowStatements.addAll(newTriples);
        List<ReifiedStatement> toBeDeleted = new ArrayList<>();
        //System.out.println("Mitte: " + currentWindowTripels.size());
        while (true) {
            if (currentWindowStatements.isEmpty()) {
                break;
            }
            if (currentWindowStatements.peek().getProperty(RDFoxWrapper.timestampProperty).getLiteral().getLong() >= goOutTime) {
                break;
            } else {
                logger.debug(Thread.currentThread().getName() + " Polled" + currentWindowStatements.size());
                toBeDeleted.add(currentWindowStatements.poll());
                logger.debug("" + currentWindowStatements.size());
            }

        }
    }

    public static void maintainStreamDatastore(Map<Statement, Long> currentWindowStatements, Map<Statement, Integer> toBeDeleted) {
        statementsToBeAdded.putAll(currentWindowStatements);
        statementsToBeDeleted.putAll(toBeDeleted);
    }

    public static void maintainStreamDatastore(Queue<ReifiedStatement> currentWindowStatements, long goOutTime, ServerConnection serverConnection, Prefixes prefixes) throws JRDFoxException {
        maintainStreamDatastore(currentWindowStatements, new ArrayList<>(), goOutTime, serverConnection, prefixes);
    }

    public static String statementsToTurtleString(Collection<Statement> input) {
        if (input == null || input.isEmpty()) {
            return "";
        }
        Model model = ModelFactory.createDefaultModel();
        //Model model = ModelFactory.createDefaultModel();
        for (Statement statement : input) {
            model.add(statement);
        }
        OutputStream out = new ByteArrayOutputStream();

        model.write(out, "TURTLE");

        return out.toString();
    }

    public void registerQuery(String query, RDFoxResultObserver rdFoxResultObserver, int i) {
        //logger.debug("Number of Queries: " + queries.values().size());
        NamedQuery namedQuery = parseNamedQuery(query);
        queries.put(rdFoxResultObserver.getQueryId() + "-" + i, namedQuery);

        namedQuery.queryAnswerMonitor = rdFoxResultObserver;
        new Thread(namedQuery).start();
        for (NamedStream stream : namedQuery.streams) {
            System.out.println("-> " + stream);
            startStreamUpdating(stream);
        }
        try {
            Thread.sleep(15);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void startStreamUpdating(NamedStream stream) {
//        new Thread(stream).start();
    }

    public Map<String, NamedQuery> getQueries() {
        return queries;
    }

    public ServerConnection getServerConnection() {
        return serverConnection;
    }

    public void createNewDatastore() {
        try {
            long before = System.currentTimeMillis();
            pause = true;
            try(OutputStream out = new FileOutputStream("tmpDatastore"); DataStoreConnection dataStoreConnection = serverConnection.newDataStoreConnection(RDFoxWrapper.datastoreName); InputStream in = new FileInputStream("tmpDatastore")) {
                dataStoreConnection.exportData(Prefixes.s_emptyPrefixes, out, "application/n-triples", new HashMap<>());
                dataStoreConnection.clear();
                dataStoreConnection.importData(UpdateType.ADDITION, Prefixes.s_emptyPrefixes, in);
            }
            pause = false;
            File file = new File("tmpDatastore");
            file.delete();
            long after = System.currentTimeMillis();
            logger.info("Refreshing took: " + (after - before) + " ms");
        } catch (JRDFoxException | IOException e) {
            e.printStackTrace();
        }
    }

    public void updateDataStoreBatch() {
        counterMethodCalled++;
        if((statementsToBeDeleted.size() != 0 || statementsToBeAdded.size() != 0) && !pause) {
            counterUpdateCalled++;
            try (DataStoreConnection dataStoreConnection = serverConnection.newDataStoreConnection(RDFoxWrapper.datastoreName)) {
                dataStoreConnection.beginTransaction(TransactionType.READ_WRITE);
                dataStoreConnection.importData(UpdateType.DELETION, Prefixes.s_emptyPrefixes, statementsToTurtleString(statementsToBeDeleted.keySet()));
                dataStoreConnection.importData(UpdateType.ADDITION, Prefixes.s_emptyPrefixes, statementsToTurtleString(statementsToBeAdded.keySet()));
                dataStoreConnection.commitTransaction();
                statementsToBeAdded.clear();
                statementsToBeDeleted.clear();
            } catch (JRDFoxException e) {
                e.printStackTrace();
            }
//            if(counterDeletion < 5) {
//                try (OutputStream outDel = new FileOutputStream("tmpDeletion" + counterDeletion++); OutputStream outAdd = new FileOutputStream("tmpAddition" + counterAddition++);) {
//                    outAdd.write(statementsToTurtleString(statementsToBeAdded.keySet()).getBytes(StandardCharsets.UTF_8));
//                    outDel.write(statementsToTurtleString(statementsToBeDeleted.keySet()).getBytes(StandardCharsets.UTF_8));
//                } catch (FileNotFoundException e) {
//                    e.printStackTrace();
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            }
        }
        /*if(counterMethodCalled % 100 == 1) {
            logger.info("Mathod Called: " + counterMethodCalled + ", " + counterUpdateCalled);
        }*/
    }


}

