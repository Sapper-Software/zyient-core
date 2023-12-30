package io.zyient.core.sdk.request;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.base.common.model.ValidationException;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class ServiceStartRequest extends Request {
    private String configFilePath;
    private String configSourceType;

    @Override
    public void validate() throws ValidationException {
        super.validate();
        ValidationException.check(this, "configFilePath");
    }
}
