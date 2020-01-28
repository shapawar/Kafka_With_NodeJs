const kafka = require('kafka-node');
const Consumer = kafka.Consumer;
const client = new kafka.KafkaClient();

consumer = new Consumer(client,
    [{ topic: 'teddy', offset: 0 }],
    {
        autoCommit: false
    }
)


consumer.on('message', function (message) {
    console.log("mes", message);
});

consumer.on('error', function (err) {
    console.log('Error:', err);
})

consumer.on('offsetOutOfRange', function (err) {
    console.log('offsetOutOfRange:', err);
})