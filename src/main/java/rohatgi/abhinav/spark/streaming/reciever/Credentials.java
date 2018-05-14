package rohatgi.abhinav.spark.streaming.reciever;

public class Credentials {

    private String accessKey;
    private String secretKey;


    public Credentials(String accessKey,String secretKey){
        this.accessKey = accessKey;
        this.secretKey = secretKey;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

}

