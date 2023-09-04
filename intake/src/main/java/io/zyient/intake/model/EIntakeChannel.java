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

public enum EIntakeChannel {
    Unknown,
    /**
     * Intake Via Email Channel
     */
    Email,
    /**
     * Intake via Literature Channel
     */
    Literature,
    /**
     * Intake Via File Channel
     */
    File,
    /**
     * Intake Via Literature File Channel
     */
    LiteratureFile,
    /**
     * Intake Via Metadata File Channel
     */
    MetadataFile,
    /**
     * Intake Via PST Email Channel
     */
    PSTEmailFile,
    /**
     * Intake Via PST Email Channel
     */
    PSTLiteratureFile,
    /**
     * Intake Via E2B channel
     */
    E2B
    ;

    public static EIntakeChannel valueOfIgnoreCase(String enumString){
        for(EIntakeChannel eIntakeChannel: EIntakeChannel.values()){
            if(eIntakeChannel.name().equalsIgnoreCase(enumString)) return eIntakeChannel;
        }
        return null;
    }

}
