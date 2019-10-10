package org.apache.rocketmq.flink.example.sku;

public class UserSkuCount {
    public UserSkuCount() {
    }

    public UserSkuCount(String userId) {
        this(userId,1);
    }

    public UserSkuCount(String userId,Integer count) {
        this.userId = userId;
        this.count=count;
    }

    private String userId;
    private Integer count;


    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "UserSkuCount{" +
                "userId='" + userId + '\'' +
                ", count=" + count +
                '}';
    }
}
