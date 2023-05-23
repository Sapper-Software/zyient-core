package ai.sapper.cdc.core.model;

import ai.sapper.cdc.common.AbstractState;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class BaseAgentState {
    public enum EAgentState {
        Unknown, Active, StandBy, Error, Stopped
    }

    public static class AgentState extends AbstractState<EAgentState> {

        public AgentState() {
            super(EAgentState.Error);
            setState(EAgentState.Unknown);
        }

        public EAgentState parseState(@NonNull String state) {
            EAgentState s = null;
            for (EAgentState ss : EAgentState.values()) {
                if (state.compareToIgnoreCase(ss.name()) == 0) {
                    s = ss;
                    break;
                }
            }
            if (s != null) {
                setState(s);
            }
            return s;
        }
    }
}
