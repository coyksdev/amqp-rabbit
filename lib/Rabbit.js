const amqp = require('amqp-connection-manager');
const uuid = require('uuid/v1');

class Rabbit {
  constructor(host) {
    this.host = host;
    this.connection = amqp.connect(host);
  }

  async listen(queue, handler, options = {}) {
    const opts = {
      durable: options.durable || false,
      noAck: options.noAck || false
    };

    this.connection.createChannel({
      json: true,
      setup: async function (channel) {
        await channel.assertQueue(queue, { durable: opts.durable });
        await channel.consume(
          queue,
          async message => {
            const ack = async () => {
              channel.ack(message);
            };
  
            const content = JSON.parse(message.content.toString());
            await Promise.resolve(await handler(content, ack));
          },
          { noAck: opts.noAck, prefetch: 1 }
        );
      },
    });
  }

  async send(queue, message, options = {}) {
    const opts = {
      durable: options.durable || false,
      persistent: options.durable || false
    };

    this.connection.createChannel({
      json: true,
      setup: async function (channel) {
        await channel.assertQueue(queue, { durable: opts.durable || false });

        await channel.sendToQueue(queue, new Buffer(JSON.stringify(message)), {
          persistent: opts.persistent || false
        });

        await channel.close();
      },
    });
  }

  async reply(queue, handler) {  
    this.connection.createChannel({
      json: true,
      setup: async function (channel) {
        await channel.assertQueue(queue, { durable: false });

        channel.consume(queue, async msg => {
          const message = await Promise.resolve(
            handler(JSON.parse(msg.content.toString()))
          );
  
          await channel.sendToQueue(
            msg.properties.replyTo,
            new Buffer.from(JSON.stringify(message)),
            {
              correlationId: msg.properties.correlationId
            }
          );
          await channel.ack(msg);
        }, { prefetch: 1 });
      },
    });
  }

  async request(queue, message) {
    return new Promise((resolve, reject) => {
      this.connection.createChannel({
        json: true,
        setup: async function (channel) {
          const q = await channel.assertQueue('', { exclusive: true, autoDelete: true });

          const correlationId = uuid();

          await channel.sendToQueue(queue, new Buffer.from(JSON.stringify(message)), {
            correlationId,
            replyTo: q.queue
          });
          
          try {
            const result =  await Promise.race([
              new Promise(async (resolve, reject) => {
                await channel.consume(
                  q.queue,
                  async msg => {
                    if (msg.properties.correlationId === correlationId) {
                      channel.close();
                      resolve(JSON.parse(msg.content.toString()));
                    }
                  },
                  { noAck: true }
                );
              }),
              new Promise((resolve, reject) => {
                setTimeout(() => {
                  reject('rpc timeout');
                }, 20000);
              })
            ]).catch(error => {
              channel.close();
              throw error;
            });
  
            resolve(result); 
          } catch (error) {
            reject(error);
          }
        }
      });
    });
  }

  async setupRequest(queue) {
    this.connection.createChannel({
      json: true,
      setup: async function (channel) {
        const q = await channel.assertQueue('', { exclusive: true });

        const waiters = {};

        channel.consume(
          q.queue,
          async msg => {
            if (waiters[msg.properties.correlationId]) {
              const go = waiters[msg.properties.correlationId];
              delete waiters[msg.properties.correlationId];
              go(JSON.parse(msg.content.toString()));
            }
          },
          { noAck: true }
        );

        return async message => {
          const correlationId = uuid();
          await channel.sendToQueue(queue, new Buffer(JSON.stringify(message)), {
            correlationId,
            replyTo: q.queue
          });
          return new Promise((resolve, reject) => {
            waiters[correlationId] = resolve;
          });
        };
      }
    });
  }

  async subscribe(exchange, handler, options = {}) {
    const opts = {
      queue: options.queue || '',
      durable: options.durable || false,
      noAck: options.noAck || true
    };

    this.connection.createChannel({
      json: true,
      setup: async function (channel) {
        await channel.assertQueue(opts.queue, { durable: opts.durable });
        
        await channel.assertExchange(exchange, 'fanout', {
          durable: opts.durable || false
        });
  
        const q = await channel.assertQueue(opts.queue, { durable: opts.durable });
  
        await channel.bindQueue(q.queue, exchange, '');
        
        await channel.consume(
          q.queue,
          async message => {
            const ack = async () => {
              channel.ack(message);
            };
  
            await Promise.resolve(handler(JSON.parse(message.content.toString()), ack));
          },
          { noAck: opts.noAck, prefetch: 1 }
        );
      },
    });
  }

  async publish(exchange, message, options = {}) {
    const opts = {
        durable: options.durable || false,
        persistent: options.durable || false
      };

    this.connection.createChannel({
      json: true,
      setup: async function (channel) {
        await channel.assertExchange(exchange, 'fanout', {
          durable: opts.durable || false
        });
  
        await channel.publish(exchange, '', new Buffer.from(JSON.stringify(message)), {
          persistent: opts.persistent || false
        });
  
        await channel.close();
      }
    });
  }
}

module.exports = Rabbit;
