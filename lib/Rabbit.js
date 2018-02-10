const amqp = require('amqplib')

class Rabbit {
  
  constructor(host) {

    this.host = host
    this.open = amqp.connect(host)
    
    const self = this
    const connectInterval = async function() {
      try {
        self.open = amqp.connect(self.host)
        const connection = await self.open
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

  async connection() {
    const conn = await this.open
    return open
  }

}

module.exports = Rabbit