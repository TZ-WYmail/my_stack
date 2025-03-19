package cn.edu.sustech.crawler;

public class User {
    private String profileImage;
    private int accountId;
    private String userType;
    private int userId;
    private String link;
    private String displayName;
    private int reputation;

    User(
            String profileImage,
            int accountId,
            String userType,
            int userId,
            String link,
            String displayName,
            int reputation) {
        this.profileImage = profileImage;
        this.accountId = accountId;
        this.userType = userType;
        this.userId = userId;
        this.link = link;
        this.displayName = displayName;
        this.reputation = reputation;
    }

    public String getProfileImage() {
        return profileImage;
    }

    public void setProfileImage(String profileImage) {
        this.profileImage = profileImage;
    }

    public int getAccountId() {
        return accountId;
    }

    public void setAccountId(int accountId) {
        this.accountId = accountId;
    }

    public String getUserType() {
        return userType;
    }

    public void setUserType(String userType) {
        this.userType = userType;
    }

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public String getLink() {
        return link;
    }

    public void setLink(String link) {
        this.link = link;
    }

    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public int getReputation() {
        return reputation;
    }

    public void setReputation(int reputation) {
        this.reputation = reputation;
    }
}
