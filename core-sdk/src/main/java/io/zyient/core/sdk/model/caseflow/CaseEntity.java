package io.zyient.core.sdk.model.caseflow;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.base.core.model.Actor;
import io.zyient.core.sdk.model.content.Content;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;
import java.util.Set;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class CaseEntity<E extends Enum<E>> {
    private String caseId;
    private String externalReferenceId;
    private String name;
    private String description;
    private E caseState;
    private Actor createdBy;
    private Actor assignedTo;
    private Actor closedBy;
    private Set<Content> documents;
    private String parentCaseId;
    private Map<String, Object> properties;
}
