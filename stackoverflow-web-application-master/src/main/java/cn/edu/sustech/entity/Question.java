package cn.edu.sustech.entity;

import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;

import java.util.Date;

@TableName("question")
public class Question {

  @TableId("question_id")
  private int questionId;

  @TableField("score")
  private int score;

  @TableField("body")
  private String body;

  @TableField("link")
  private String link;

  @TableField("answer_count")
  private int answerCount;

  @TableField("view_count")
  private int viewCount;

  @TableField("content_license")
  private String contentLicense;

  @TableField("title")
  private String title;

  @TableField("last_activity_date")
  private Date lastActivityDate;

  @TableField("last_edit_date")
  private Date lastEditDate;

  @TableField("creation_date")
  private Date creationDate;

  @TableField("account_id")
  private int accountId;

  public int getQuestionId() {
    return questionId;
  }

  public void setQuestionId(int id) {
    this.questionId = id;
  }

  public int getScore() {
    return score;
  }

  public void setScore(int score) {
    this.score = score;
  }

  public String getBody() {
    return body;
  }

  public void setBody(String body) {
    this.body = body;
  }

  public String getLink() {
    return link;
  }

  public void setLink(String link) {
    this.link = link;
  }

  public int getAnswerCount() {
    return answerCount;
  }

  public void setAnswerCount(int answerCount) {
    this.answerCount = answerCount;
  }

  public int getViewCount() {
    return viewCount;
  }

  public void setViewCount(int viewCount) {
    this.viewCount = viewCount;
  }

  public String getContentLicense() {
    return contentLicense;
  }

  public void setContentLicense(String contentLicense) {
    this.contentLicense = contentLicense;
  }

  public String getTitle() {
    return title;
  }

  public void setTitle(String title) {
    this.title = title;
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

  public int getAccountId() {
    return accountId;
  }

  public void setAccountId(int accountId) {
    this.accountId = accountId;
  }
}
