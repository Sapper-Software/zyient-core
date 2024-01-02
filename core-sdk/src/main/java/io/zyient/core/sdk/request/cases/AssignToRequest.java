package io.zyient.core.sdk.request.cases;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.base.core.model.UserOrRole;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class AssignToRequest extends BaseCaseRequest {
    private UserOrRole assignTo;
}
