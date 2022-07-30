package MainFiles;

import IO.Serialization.TransferObject;
import IO.Serialization.VariableBindings;
import RDFox.RDFoxWrapper;
import tech.oxfordsemantic.jrdfox.client.*;
import tech.oxfordsemantic.jrdfox.exceptions.JRDFoxException;

import java.util.ArrayList;
import java.util.List;

public class RDFoxResultObserver implements QueryAnswerMonitor {
    private final String queryId;
    private final ServerConnection sCon;

    public RDFoxResultObserver(String queryId, ServerConnection sCon) {
        this.queryId = queryId;
        this.sCon = sCon;
    }

    @Override
    public void processQueryAnswer(List<ResourceValue> list, long l) throws JRDFoxException {
        //System.out.println("Receiving");
        long timestamp = System.currentTimeMillis();
        List<VariableBindings> timestampedVariableBindings = new ArrayList<>();
        List<String> bindings = new ArrayList<>();

        String[] resultArr = list.stream().map(ResourceValue::getLexicalForm).toArray(String[]::new);
        for (int i = 0; i < resultArr.length; i++)
            bindings.add(resultArr[i]);

        timestampedVariableBindings.add(new VariableBindings(bindings));
        double usedMB = getCurrentMemoryUsage();
        RDFoxRunningExample.answers.add(new TransferObject(queryId.split("-")[0], timestampedVariableBindings, usedMB, timestamp));
    }

    private double getCurrentMemoryUsage() {
        System.gc();
        Runtime rt = Runtime.getRuntime();
        double mem_usage_mb;
        double java_env = ((rt.totalMemory() - rt.freeMemory()) / 1024.0 / 1024.0);

        try(DataStoreConnection dCon = sCon.newDataStoreConnection(RDFoxWrapper.datastoreName)) {
            ComponentInfo ci = dCon.getComponentInfo(true);
            double serverSize = Double.valueOf((Long) ci.getPropertyValues().get("Aggregate size")) / 1024.0 / 1024.0;
            mem_usage_mb = java_env + serverSize;
        } catch (JRDFoxException e) {
            throw new RuntimeException(e);
        }
        return mem_usage_mb;
    }

    @Override
    public void queryAnswersStarted(String[] strings) throws JRDFoxException {

    }

    @Override
    public void queryAnswersFinished() throws JRDFoxException {

    }

    public String getQueryId() {
        return queryId;
    }
}