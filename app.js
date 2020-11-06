const {Kafka} = require('kafkajs');
const request = require('request');

const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['localhost:9092']
});

const consumer = kafka.consumer({groupId: 'test-group'});

const consume = async (topic, fromBeginning) => {
    // Consuming
    await consumer.connect();
    await consumer.subscribe({topic, fromBeginning});

    await consumer.run({
        eachMessage: async ({topic, partition, message}) => {
            // console.log({
            //     partition,
            //     offset: message.offset,
            //     value: message.value.toString(),
            // });
            if (JSON.parse(message.value.toString()).hasOwnProperty('commentData')) {
                request.post({
                    url: 'http://localhost:3000/micro-service/comment-notification',
                    headers: {
                        'Authorization': 'Bearer @#*ey974as93NalMLGasdf3632SL$#!',
                        'Content-Type': 'application/json'
                    },
                    body: message.value.toString()
                }, (error, response, body) => {
                    if (error) {
                        return console.log(error);
                    }
                    if (response.hasOwnProperty('statusCode')) {
                        if (response.statusCode === 200) {
                            console.log('Success!')
                        } else {
                            console.log(response.statusCode);
                        }
                    }
                });
            }
        },
    })
};

consume('bsocial', false).catch(console.error);
