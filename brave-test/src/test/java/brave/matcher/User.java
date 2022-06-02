package brave.matcher;

public class User {
    private Long id;
    private Long tikiId;
    private String phone;
    private String email;
    private String name;

    public User setId(Long id) {
        this.id = id;
        return this;
    }

    public User setTikiId(Long tikiId) {
        this.tikiId = tikiId;
        return this;
    }

    public User setPhone(String phone) {
        this.phone = phone;
        return this;
    }

    public User setEmail(String email) {
        this.email = email;
        return this;
    }

    public User setName(String name) {
        this.name = name;
        return this;
    }

    public Long getId() {
        return id;
    }

    public Long getTikiId() {
        return tikiId;
    }

    public String getPhone() {
        return phone;
    }

    public String getEmail() {
        return email;
    }

    public String getName() {
        return name;
    }
}
