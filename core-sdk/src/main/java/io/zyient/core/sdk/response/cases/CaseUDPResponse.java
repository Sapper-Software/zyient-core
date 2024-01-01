package io.zyient.core.sdk.response.cases;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.core.sdk.request.cases.CaseUDPRequest;
import io.zyient.core.sdk.response.Response;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class CaseUDPResponse extends Response<CaseUDPRequest> {
    private String caseId;
    private String json;

    public CaseUDPResponse() {}

    public CaseUDPResponse(@NonNull Exception error) {
        setError(error);
    }
}
