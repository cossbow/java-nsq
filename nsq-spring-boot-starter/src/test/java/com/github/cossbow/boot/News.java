package com.github.cossbow.boot;


import java.io.Serializable;
import java.time.Instant;
import java.time.OffsetDateTime;


public class News implements Serializable {
    private static final long serialVersionUID = 1052075161334157354L;

    private int id;
    private String title;
    private String content;
    private Instant createdTime;
    private OffsetDateTime publishedTime;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public Instant getCreatedTime() {
        return createdTime;
    }

    public void setCreatedTime(Instant createdTime) {
        this.createdTime = createdTime;
    }

    public OffsetDateTime getPublishedTime() {
        return publishedTime;
    }

    public void setPublishedTime(OffsetDateTime publishedTime) {
        this.publishedTime = publishedTime;
    }

    @Override
    public String toString() {
        return "News{" +
                "id=" + id +
                ", title='" + title + '\'' +
                ", content='" + content + '\'' +
                ", createdTime=" + createdTime +
                ", publishedTime=" + publishedTime +
                '}';
    }
}
