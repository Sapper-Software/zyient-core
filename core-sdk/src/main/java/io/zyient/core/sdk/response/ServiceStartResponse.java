package io.zyient.core.sdk.response;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.core.sdk.request.ServiceStartRequest;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class ServiceStartResponse<E extends Enum<E>> extends Response<ServiceStartRequest> {
    private E state;
    private String name;

    public ServiceStartResponse() {}

    public ServiceStartResponse(@NonNull Exception error) {
        setError(error);
    }
}
