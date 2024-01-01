package io.zyient.core.sdk.request.cases;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.core.sdk.request.Request;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class BaseCaseRequest extends Request {
    private String comments;
}
