package io.zyient.core.sdk.response;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.core.sdk.request.Request;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class ServiceStatusResponse<E extends Enum<E>> extends Response<Request.VoidRequest> {
    private E state;
    private String name;

    public ServiceStatusResponse() {
    }

    public ServiceStatusResponse(@NonNull Exception error) {
        setError(error);
    }
}
