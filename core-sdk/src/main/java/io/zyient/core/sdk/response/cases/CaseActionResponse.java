package io.zyient.core.sdk.response.cases;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.core.sdk.model.caseflow.CaseEntity;
import io.zyient.core.sdk.request.Request;
import io.zyient.core.sdk.response.Response;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class CaseActionResponse<R extends Request, E extends Enum<E>> extends Response<R> {
    private CaseEntity<E> caseEntity;

    public CaseActionResponse() {
    }

    public CaseActionResponse(@NonNull Exception error) {
        setError(error);
    }
}
