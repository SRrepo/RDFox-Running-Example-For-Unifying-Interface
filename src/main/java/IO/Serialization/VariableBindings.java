package IO.Serialization;

import java.util.List;

public class VariableBindings {
    private List<String> variableValuesAsString;

    public List<String> getVariableValuesAsString() {
        return variableValuesAsString;
    }

    public VariableBindings(List<String> variableValuesAsString) {
        this.variableValuesAsString = variableValuesAsString;
    }
}
