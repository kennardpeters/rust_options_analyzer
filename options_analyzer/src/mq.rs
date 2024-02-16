extern crate amqprs;
//extern crate tracing_subscriber;
use amqprs::{
    callbacks::{DefaultChannelCallback, DefaultConnectionCallback},
    channel::{
        BasicConsumeArguments, BasicPublishArguments, QueueBindArguments, QueueDeclareArguments,
    },
    connection::{Connection, OpenConnectionArguments},
    consumer::DefaultConsumer,
    BasicProperties,
};
use tokio::time;

//use tracing_subscriber::{fmt, prelude::*, EnvFilter};

pub async fn publish_example() {

    //tracing_subscriber::registry()
    //    .with(fmt::layer())
    //    .with(EnvFilter::from_default_env())
    //    .try_init()
    //    .ok();


    let connection = Connection::open(&OpenConnectionArguments::new(
        //"localhost",
        "host.docker.internal",
        5672,
        "guest",
        "guest",
    ))
    .await
    .unwrap();

    connection.register_callback(DefaultConnectionCallback)
    .await
    .unwrap();

    //open a channel to the connection
    let channel = connection.open_channel(None).await.unwrap();
    channel
        .register_callback(DefaultChannelCallback)
        .await
        .unwrap();

    // declare a queue
    let (queue_name, _, _) = channel
        .queue_declare(QueueDeclareArguments::durable_client_named(
            "amqprs.examples.basic",
        ))
        .await
        .unwrap()
        .unwrap();

    // bind the queue to exchange
    let routing_key = "amqprs.example";
    let exchange_name = "amq.topic";
    channel
        .queue_bind(QueueBindArguments::new(
            &queue_name,
            exchange_name,
            routing_key,
        ))
        .await
        .unwrap();
    ///////////////////////////////////////////////
    let args = BasicConsumeArguments::new(
        &queue_name,
        "example_basic_pub_sub"
    );

    channel
        .basic_consume(DefaultConsumer::new(args.no_ack), args)
        .await
        .unwrap();

    // publish message
    let content = String::from(
        r#"
            {
                "publisher": "example"
                "data": "Hello, amqprs!"
            }
        "#,
    ).into_bytes();

    //create arguments for basic_publish
    let args = BasicPublishArguments::new(exchange_name, routing_key);

    channel
        .basic_publish(BasicProperties::default(), content, args)
        .await
        .unwrap();


    time::sleep(time::Duration::from_secs(1)).await;
    //explicitly close
    channel.close().await.unwrap();
    connection.close().await.unwrap();
}
