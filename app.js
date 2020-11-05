const {Kafka} = require('kafkajs');

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
            console.log({
                partition,
                offset: message.offset,
                value: message.value.toString(),
            });
        },
    })
};

consume('bsocial', false).catch(console.error);
