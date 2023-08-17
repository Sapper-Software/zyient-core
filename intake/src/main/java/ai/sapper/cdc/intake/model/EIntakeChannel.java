package ai.sapper.cdc.intake.model;

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
