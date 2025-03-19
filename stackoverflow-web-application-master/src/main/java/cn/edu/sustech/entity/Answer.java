package cn.edu.sustech.entity;

import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;

import java.util.Date;

@TableName("answer")
public class Answer {

  @TableId("answer_id")
  private int answerId;

  @TableField("last_activity_date")
  private Date lastActivityDate;

  @TableField("last_edit_date")
  private Date lastEditDate;

  @TableField("creation_date")
  private Date creationDate;

  @TableField("score")
  private int score;

  @TableField("is_accepted")
  private boolean isAccepted;

  @TableField("content_license")
  private String contentLicense;

  @TableField("question_id")
  private int questionId;

  @TableField("body")
  private String body;

  @TableField("account_id")
  private int accountId;

  public int getAnswerId() {
    return answerId;
  }

  public void setAnswerId(int answerId) {
    this.answerId = answerId;
  }

  public Date getLastActivityDate() {
    return lastActivityDate;
  }

  public void setLastActivityDate(Date lastActivityDate) {
    this.lastActivityDate = lastActivityDate;
  }

  public Date getLastEditDate() {
    return lastEditDate;
  }

  public void setLastEditDate(Date lastEditDate) {
    this.lastEditDate = lastEditDate;
  }

  public Date getCreationDate() {
    return creationDate;
  }

  public void setCreationDate(Date creationDate) {
    this.creationDate = creationDate;
  }

  public int getScore() {
    return score;
  }

  public void setScore(int score) {
    this.score = score;
  }

  public boolean isAccepted() {
    return isAccepted;
  }

  public void setAccepted(boolean accepted) {
    isAccepted = accepted;
  }

  public String getContentLicense() {
    return contentLicense;
  }

  public void setContentLicense(String contentLicense) {
    this.contentLicense = contentLicense;
  }

  public int getQuestionId() {
    return questionId;
  }

  public void setQuestionId(int questionId) {
    this.questionId = questionId;
  }

  public String getBody() {
    return body;
  }

  public void setBody(String body) {
    this.body = body;
  }

  public int getAccountId() {
    return accountId;
  }

  public void setAccountId(int accountId) {
    this.accountId = accountId;
  }
}
