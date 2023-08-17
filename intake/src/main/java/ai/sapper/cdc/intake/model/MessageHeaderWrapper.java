package ai.sapper.cdc.intake.model;

import lombok.Data;

import java.util.Date;

@Data
public class MessageHeaderWrapper {
    private String id;
    private String[] from;
   	private String[] to;
    private String[] cc;
    private String[] bcc;
    private String[] replyTo;
    private Date sendDate;
    private Date receivedDate;
    private String subject;
  
    
}
