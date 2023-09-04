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

package io.zyient.intake.model;

public enum EmailType {
    Unknown,
    /**
     * Email Intake
     */
    Inbound,
    /**
     * Sender Email
     */
    Outbound,
    /**
     * Both Email Intake and Sender Email
     */
    Both;

    /**
     * Returns the Enum value that matches with the input string. This is case insensitive
     * @param enumString string value to be treated as a case insensitive input
     * @return
     */
    public static EmailType valueOfIgnoreCase(String enumString){
        for(EmailType emailType: EmailType.values()){
            if(emailType.name().equalsIgnoreCase(enumString)) return emailType;
        }
        return null;
    }
}