package cloudfoundry.services.cloudamqp;

import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class Main {
    public static void main(String[] args) throws Exception {
        AnnotationConfigApplicationContext applicationContext = new AnnotationConfigApplicationContext(AmqpConfiguration.class);
        applicationContext.registerShutdownHook();
        applicationContext.start();
    }
}
