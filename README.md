1> MiniPubSub

```bash 
cd mini-ps/
```

```bash 
docker build --no-cache -t mini-pubsub:latest -f Dockerfile .
```

```bash 
docker compose up -d
```

2> MiniPubSub-Client

```bash 
cd mini-ps-client/
```

```bash 
mvn clean compile
```

```bash 
mvn clean install -DskipTests
```

3> Spring Boot Integration

```java
<dependency>
    <groupId>org.mini.pubsub</groupId>
    <artifactId>minipubsub-client</artifactId>
    <version>1.0.0</version>
</dependency>
```

```java
mini.pubsub.server.host=127.0.0.1
mini.pubsub.server.port=9809
mini.pubsub.server.connect-timeout-ms=5000
mini.pubsub.server.auto-reconnect=true
```

- Config

```java
@Configuration
public class MiniPubSubConfig {
    @Value("${mini.pubsub.server.host}")
    private String host;

    @Value("${mini.pubsub.server.port}")
    private int port;

    @Value("${mini.pubsub.server.connect-timeout-ms:5000}")
    private int connectTimeoutMs;

    @Value("${mini.pubsub.server.auto-reconnect:true}")
    private boolean autoReconnect;

    @Bean(destroyMethod = "close")
    public PubSubClient pubSubClient() throws InterruptedException {
        ClientConfig config = new ClientConfig(host, port, connectTimeoutMs, autoReconnect);
        PubSubClient client = new PubSubClient(config);

        client.connect();
        return client;
    }
}
```

```java
@Component
public class MiniPubSubListenerProcessor implements BeanPostProcessor {

    private final PubSubClient client;

    public MiniPubSubListenerProcessor(PubSubClient client) {
        this.client = client;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) {
        for (Method method : bean.getClass().getDeclaredMethods()) {
            if (method.isAnnotationPresent(PubSubListener.class)) {
                PubSubListener annotation = method.getAnnotation(PubSubListener.class);
                String topic = annotation.topic();

                client.subscribe(topic, (t, payload) -> {
                    try {
                        method.setAccessible(true);
                        method.invoke(bean, (Object) payload);
                    } catch (Exception ignored) {
                    }
                });
            }
        }
        return bean;
    }
}
```

- Publisher

```java
 public int publish(String message) {
    pubSubClient.publish("notifications", message);
    return 1;
}
```

- Listener

```java
@PubSubListener(topic = "notifications")
public void onPaymentSuccess(byte[] payload) {
    String data = new String(payload, StandardCharsets.UTF_8);
    System.out.println(data);
}
```