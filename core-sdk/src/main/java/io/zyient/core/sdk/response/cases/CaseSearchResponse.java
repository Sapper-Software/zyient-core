package io.zyient.core.sdk.response.cases;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.core.sdk.model.caseflow.CaseEntity;
import io.zyient.core.sdk.request.cases.CaseSearchRequest;
import io.zyient.core.sdk.response.Response;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class CaseSearchResponse<E extends Enum<E>> extends Response<CaseSearchRequest> {
    private int caseCount = 0;
    private List<CaseEntity<E>> cases;

    public CaseSearchResponse() {}

    public CaseSearchResponse(@NonNull Exception error) {
        setError(error);
    }
}
