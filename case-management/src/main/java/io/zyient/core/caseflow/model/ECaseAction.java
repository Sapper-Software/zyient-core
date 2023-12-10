/*
 * Copyright(C) (2023) Sapper Inc. (open.source at zyient dot io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.zyient.core.caseflow.model;

import io.zyient.base.core.model.StringKey;
import io.zyient.core.caseflow.model.CaseAction;

public enum ECaseAction {

    Create(CaseAction.formatActionId(1), "Action - Create new Case."),
    Update(CaseAction.formatActionId(2), "Action - Update open cases."),
    Close(CaseAction.formatActionId(3), "Action - Close open cases."),
    UpdateState(CaseAction.formatActionId(4), "Action - Update state of Case."),
    Delete(CaseAction.formatActionId(5), "Action - Delete cases."),
    Comment(CaseAction.formatActionId(6), "Action - Comment on this specific case."),
    CommentRespond(CaseAction.formatActionId(7), "Action - Respond to a comment on a specific case."),
    CommentClose(CaseAction.formatActionId(8), "Action - Resolve/Close a comment on a specific case."),
    AddArtefact(CaseAction.formatActionId(9), "Action - Add artefacts/documents to a case."),
    UpdateArtefact(CaseAction.formatActionId(10), "Action - Update artefacts/documents for a case."),
    DeleteArtefact(CaseAction.formatActionId(11), "Action - Delete/Remove artefacts/documents from a case."),
    AssignTo(CaseAction.formatActionId(12), "Action - Assign a case to a specific user."),
    RemoveAssignment(CaseAction.formatActionId(13), "Action - Remove a case assignment.");

    private final CaseAction action;

    ECaseAction(String code, String description) {
        action = new CaseAction();
        action.setKey(new StringKey(code));
        action.setDescription(description);
    }

    public CaseAction action() {
        return action;
    }
}
