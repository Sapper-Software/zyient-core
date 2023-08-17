package ai.sapper.cdc.intake.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class EmailConnectionsResponse<T> {
    private String status;
    private String message;
    private T data;
}
