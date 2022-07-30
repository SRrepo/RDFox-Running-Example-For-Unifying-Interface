package MainFiles;

import RDFox.RDFoxWrapper;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Statement;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class RDFoxStreamReceiver implements Runnable {
    private String iri;
    private InputStream in;
    Socket socket;

    public RDFoxStreamReceiver(String iri, int portNumber) {
        this.iri = iri;
        try {
            System.out.println("Host: " + iri.split(":")[0] + iri.split(":")[1]);
            socket = new Socket("127.0.0.1", portNumber);
            in = socket.getInputStream();
        } catch (IOException e) {
            System.out.println("Could not establish Socket Connection.");
            e.printStackTrace();
        }

    }

    @Override
    public void run() {
        try {
            BufferedInputStream bi = new BufferedInputStream(in);
            BufferedReader in = new BufferedReader(new InputStreamReader(bi));
            loop:while (true) {
                int counter = 0;
                while (!in.ready()) {
                    if(counter == 400) {
                        break loop;
                    }
                    counter++;
                    Thread.sleep(10);
                }
                //long startTime = System.currentTimeMillis();
                StringBuilder result = new StringBuilder();
                while (!result.toString().endsWith(".\n")) {
                    result.append(in.readLine()).append("\n");
                }
                Model model = ModelFactory.createDefaultModel();
                RDFDataMgr.read(model, new ByteArrayInputStream(result.toString().getBytes(StandardCharsets.UTF_8)), Lang.TURTLE);

                for (Statement st : model.listStatements().toList()) {
                    RDFoxWrapper.getRDFoxWrapper().putData(iri, st);
                }
                RDFoxWrapper.getRDFoxWrapper().flushIfNecessary(iri);
            }
            System.out.println("Ending Stream: " + iri);
            RDFoxRunningExample.streamsRunning = false;
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }
    }
}
