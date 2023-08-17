package ai.sapper.cdc.intake.model;

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