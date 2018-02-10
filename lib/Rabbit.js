const amqp = require('amqplib')

class Rabbit {
  
  constructor(host) {

    this.host = host
    this.open = amqp.connect(host)
    
    this.listenHandlers = []

    const self = this
    const connectInterval = async function() {
      try {
        self.open = amqp.connect(self.host)
        const connection = await self.open
        
        for(let i = 0; i < self.listenHandlers.length; i++) {
          const listenHandler = self.listenHandlers[i]
          await self.listen(listenHandler.queue, listenHandler.handler)
        }

        connection.on('close', onClose)
        clearInterval(this)
      } catch (error) {
        console.log(`Rabbit is trying to reconnect at ${self.host}`)
      }
    }
    
    const onClose = () => {
      setInterval(connectInterval, 3000)
    }
    
    this.open.then(connection => {
      connection.on('close', onClose)
    })
    
  }

  async listen(queue, handler) {
    try {
      this.listenHandlers.push({ queue, handler })
      
      const conn = await this.open
      const channel = await conn.createChannel()

      await channel.assertQueue(queue, { durable: false })
      
      channel.consume(queue, async (message) => {
        await Promise.resolve(handler(JSON.parse(message.content)))
        channel.ack(message)
      })
    } catch (error) {
      throw error
    }
  }

  async send(queue, message) {
    try {
      const conn = await this.open
      const channel = await conn.createChannel()

      await channel.assertQueue(queue, { durable: false })
      channel.sendToQueue(queue, new Buffer(JSON.stringify(message)));
    } catch (error) {
      throw error
    }
  }
}

module.exports = Rabbit