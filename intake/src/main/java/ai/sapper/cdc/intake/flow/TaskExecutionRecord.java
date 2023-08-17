package ai.sapper.cdc.intake.flow;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Data;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;

@Data
public class TaskExecutionRecord<T> {
    private long startTime;
    private long endTime;
    private T record;
    private Map<String, TaskResponse> responses = new HashMap<>();
    @JsonIgnore
    private TaskContext context;

    public void addResponse(@Nonnull String name, @Nonnull TaskResponse response) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name));
        responses.put(name, response);
    }
}
