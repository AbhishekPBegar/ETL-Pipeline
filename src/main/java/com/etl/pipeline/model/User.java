package com.etl.pipeline.model;

public class User {
    private Integer id;
    private String name;
    private Integer age;
    private String city;
    private String source;
    
    // Default constructor
    public User() {}
    
    // Constructor without ID (for new records)
    public User(String name, Integer age, String city, String source) {
        this.name = name;
        this.age = age;
        this.city = city;
        this.source = source;
    }
    
    // Full constructor
    public User(Integer id, String name, Integer age, String city, String source) {
        this.id = id;
        this.name = name;
        this.age = age;
        this.city = city;
        this.source = source;
    }
    
    // Getters and Setters
    public Integer getId() { return id; }
    public void setId(Integer id) { this.id = id; }
    
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    
    public Integer getAge() { return age; }
    public void setAge(Integer age) { this.age = age; }
    
    public String getCity() { return city; }
    public void setCity(String city) { this.city = city; }
    
    public String getSource() { return source; }
    public void setSource(String source) { this.source = source; }
    
    @Override
    public String toString() {
        return String.format("User{id=%d, name='%s', age=%d, city='%s', source='%s'}", 
                           id, name, age, city, source);
    }
}
