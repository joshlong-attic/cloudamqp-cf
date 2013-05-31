package cloudfoundry.services.cloudamqp;


import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.*;
import org.springframework.amqp.rabbit.core.*;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.transaction.RabbitTransactionManager;
import org.springframework.amqp.support.converter.*;
import org.springframework.context.annotation.*;
import org.springframework.core.task.*;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ConcurrentTaskScheduler;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import javax.annotation.PostConstruct;
import java.io.*;
import java.net.URI;
import java.util.Iterator;
import java.util.concurrent.Executors;

@Configuration
@PropertySource("/services.properties")
@EnableTransactionManagement
public class AmqpConfiguration {

    private static String VCAP_SERVICES = "{\"cloudamqp-dev-n/a\":[{\"name\":\"cloudamqp-dev-7f870\",\"label\":\"cloudamqp-dev-n/a\",\"plan\":\"lemur\",\"credentials\":{\"uri\":\"amqp://sjuaygxk:iWba0HKZ65PinlTyxXazuIdf2YnqBvZE@lemur.cloudamqp.com/sjuaygxk\"}}]}";
    private String registrationQueue = "registrations";
    private String registrationExchange = this.registrationQueue;

    private String uriFromVcapServices(String vcapServicesString) throws Throwable {
        ObjectMapper mapper = new ObjectMapper();

        BufferedReader stringWriter = new BufferedReader(new StringReader(vcapServicesString));
        JsonNode rootNode = mapper.readTree(stringWriter);
        Iterator<String> fieldNames = rootNode.getFieldNames();
        String fieldName;

        if (!(fieldNames.hasNext()))
            return null;
        fieldName = fieldNames.next();
        JsonNode informationAboutServiceNode = rootNode.get(fieldName);
        JsonNode credentials = informationAboutServiceNode.get(0).get("credentials");
        return credentials.get("uri").asText();
    }

  //  @PostConstruct
    public void run() throws Throwable {

        for (String k : System.getenv().keySet())
            log(k + '=' + System.getenv(k));

        for (Object k : System.getProperties().keySet())
             log("" + k + '=' + System.getProperties().getProperty("" + k));
    }

    @Bean
    public TaskScheduler taskScheduler() {
        return new ConcurrentTaskScheduler(Executors.newScheduledThreadPool(10));
    }

    @Bean
    public SimpleAsyncTaskExecutor taskExecutor() {
        return new SimpleAsyncTaskExecutor();
    }

    @Bean
    public RabbitTemplate rabbitTemplate(MessageConverter messageConverter, ConnectionFactory connectionFactory) {
        RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
        rabbitTemplate.setMessageConverter(messageConverter);
        return rabbitTemplate;
    }

    @Bean
    public RabbitTransactionManager amqpTransactionManager(ConnectionFactory connectionFactory) {
        return new RabbitTransactionManager(connectionFactory);
    }

    @Bean
    public JsonMessageConverter jsonMessageConverter() {
        return new JsonMessageConverter();
    }

    ConnectionFactory connectionFactory(String user, String pw, String host , int port,  String  vhost  ) throws Throwable {
        CachingConnectionFactory cachingConnectionFactory = new CachingConnectionFactory();
        cachingConnectionFactory.setUsername(user);
        cachingConnectionFactory.setVirtualHost( vhost );
        cachingConnectionFactory.setPassword(pw);
        cachingConnectionFactory.setHost(host);
        cachingConnectionFactory.setPort(port);
        return cachingConnectionFactory;
    }

    static void log (String msg, Object ... p ) throws Throwable {
     System.out.println( String.format(msg, p));
    }
    @Bean
    public ConnectionFactory connectionFactory() throws Throwable {
        String vcapServicesEnvVariable = System.getenv("VCAP_SERVICES");

        String uri = this.uriFromVcapServices(vcapServicesEnvVariable);

        log("uri="+ uri );

        URI uriObj = new URI(uri);
        String userInfoParts[] = uriObj.getUserInfo().split(":");
        String user = userInfoParts[0];
        String pw = userInfoParts[1];
        String host = uriObj.getHost();
        String vHost = uriObj.getPath();
        int port = uriObj.getPort();
        return connectionFactory(user, pw, host ,  port ,vHost  );
    }

    @Bean
    public AmqpAdmin amqpAdmin(ConnectionFactory connectionFactory) {
        return new RabbitAdmin(connectionFactory);
    }

    @Bean
    public Queue customerQueue(AmqpAdmin amqpAdmin) {
        Queue q = new Queue(this.registrationQueue);
        amqpAdmin.declareQueue(q);
        return q;
    }

    @Bean
    public DirectExchange customerExchange(AmqpAdmin amqpAdmin) {
        DirectExchange directExchange = new DirectExchange(registrationQueue);
        amqpAdmin.declareExchange(directExchange);
        return directExchange;
    }

    @Bean
    public Binding marketDataBinding(Queue customerQueue, DirectExchange directExchange) {
        return BindingBuilder
                .bind(customerQueue)
                .to(directExchange)
                .with(this.registrationQueue);
    }

    @Bean
    public SimpleMessageListenerContainer simpleMessageListenerContainer(RegistrationMessageListener ml,
                                                                         TaskExecutor taskExecutor,
                                                                         PlatformTransactionManager transactionManager,
                                                                         Queue customerQueue,
                                                                         ConnectionFactory connectionFactory) {
        int max = 1;
        SimpleMessageListenerContainer smlc = new SimpleMessageListenerContainer();
        smlc.setMessageListener(ml);
        smlc.setTaskExecutor(taskExecutor);
        smlc.setAutoStartup(true);
        smlc.setQueues(customerQueue);
        smlc.setConcurrentConsumers(max);
        smlc.setTransactionManager(transactionManager);
        smlc.setConnectionFactory(connectionFactory);
        return smlc;
    }

    @Bean
    public RegistrationMessageListener messageListener() {
        return new RegistrationMessageListener();
    }

    @Bean
    public RegistrationMessageProducer messageProducer(RabbitTemplate rabbitTemplate) {
        return new RegistrationMessageProducer(rabbitTemplate, this.registrationExchange);
    }
}

// listens for messages coming in
class RegistrationMessageListener implements org.springframework.amqp.core.MessageListener {
    @Override
    public void onMessage(Message message) {
        System.out.println("Received new message: " + message.toString());
    }
}

class RegistrationMessageProducer {
    private RabbitTemplate rabbitTemplate;
    private String registrationExchange;

    public RegistrationMessageProducer(RabbitTemplate rabbitTemplate, String registrationExchange) {
        this.registrationExchange = registrationExchange;
        this.rabbitTemplate = rabbitTemplate;
    }

    @PostConstruct
    public void createSomeRegistrationsToProcess() throws Throwable {
        for (int i = 0; i < 10; i++) {
            Registration registration = new Registration(i, i + "@email.com", (i % 2 == 0 ? "john" : "jane") + " doe");
            rabbitTemplate.convertAndSend(this.registrationExchange, registration);
        }
    }
}
