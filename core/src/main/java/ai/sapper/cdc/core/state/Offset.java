package ai.sapper.cdc.core.state;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(
        use = JsonTypeInfo.Id.CLASS
)
public abstract class Offset implements Comparable<Offset> {
    public abstract String asString();

    public abstract Offset fromString(@NonNull String source) throws Exception;
}
