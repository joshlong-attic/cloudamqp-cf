package cloudfoundry.services.cloudamqp;

public class Registration {
    private long customerId;
    private String email, name;

    @Override
    public String toString() {
        return "Registration{" +
                "customerId=" + customerId +
                ", email='" + email + '\'' +
                ", name='" + name + '\'' +
                '}';
    }

    public Registration(long customerId, String e, String n) {
        this.email = e;
        this.name = n;
        this.customerId = customerId;
    }

    public long getCustomerId() {
        return this.customerId;
    }

    public String getName() {
        return this.name;
    }

    public String getEmail() {
        return this.email;
    }
}
