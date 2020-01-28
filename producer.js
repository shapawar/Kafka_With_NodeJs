const express = require('express');
const kafka = require('kafka-node');
const bodyparser = require('body-parser');
const config = require('./config/config');

const app = express();

app.use(bodyparser.json());
app.use(bodyparser.urlencoded({ extended: true }));

//Create Producer 

const Producer = kafka.Producer;
const client = new kafka.KafkaClient();
const producer = new Producer(client);

producer.on('ready', () => {
    console.log('producer is ready');

});

producer.on('error', (err) => {
    console.log("producer is in error state");
    console.log(err.message);
});

app.get('/', (req, res, next) => {
    res.json({ greeting: 'kafks consumer' });
});

app.post('/sendMsg', (req, res, next) => {
    var sendmsg = req.body.message;
    payloads = [
        {
            topic: req.body.topic, messages: sendmsg, partition: 0
        }
    ];

   // console.log("check message", JSON.stringify(payloads));
    producer.send(payloads, (err, data) => {
        if (err) {
            res.json(err.message)
        }
        res.json(data)
    });


});


app.listen(5001, () => {
    console.log("kafka producer running at 5001");
});