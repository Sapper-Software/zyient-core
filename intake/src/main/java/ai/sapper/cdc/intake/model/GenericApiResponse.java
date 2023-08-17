package ai.sapper.cdc.intake.model;

import lombok.Getter;
import lombok.Setter;


@Getter
@Setter
public class GenericApiResponse<T> {

    private String status;
    private String message;
    private T data;

    public GenericApiResponse(String status, String message, T data) {
        this.setStatus(status);
        this.setMessage(message);
        this.setData(data);
    }

    public GenericApiResponse(String status, String message) {
        this.setStatus(status);
        this.setMessage(message);
    }


}



