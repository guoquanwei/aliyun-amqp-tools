# amqp-tools

<u>just support ES6

## Install

This is a [Node.js](https://nodejs.org/en/) module available through the
[npm registry](https://www.npmjs.com/). Installation is done using the
[`npm install` command](https://docs.npmjs.com/getting-started/installing-npm-packages-locally):

```bash
$ npm install aliyun-amqp-tools
```

## Example
```js
const aliConfig = {
    accessKeyId: 'xxxxx',
    accessKeySecret: 'xxxxx',
    resourceOwnerId: 'xxxxx',
    host: 'xxxxx'
    // 可追加属性
}
const amqpHelper = require('aliyun-amqp-tools')(aliConfig)

const queue = {
    name: 'xxxxx',
    vhostName: 'xxxxx',
    exchange: 'xxxx', // fanout queue is required!
    prefetch: 1 // default 1
}
const consumeFuc = async (msg) => {}
await amqpHelper.startProduce(queue, 'work' || 'fanout')
await amqpHelper.startConsume(queue, consumeFuc, 'work' || 'fanout')

await amqpHelper.sendMsg(queue, { anyKey: 'anyValue' }, 'work' || 'fanout')
```

## API

The API of this module is intended to be similar to the
[Node.js `aliyun-amqp-node-cli` module](https://github.com/AliwareMQ/amqp-demos/tree/master/amqp-node-demo?spm=a2c4g.11186623.2.12.1465618foxqNxZ).

### startProduce([queue], [type])
- queue
```js
queue = {
    name: 'xxxxx',
    vhostName: 'xxxxx',
    exchange: 'xxxx', // fanout queue is required!
    prefetch: 1 // default 1
}
```

### startConsume([queue], [consumeFuc], [type])
- consumeFuc
```js
consumeFuc = async (msg) => {

}
```

### sendMsg([queue], [msgObj || msgArray], [type])
You must init connect before sendMsg.

### getConn([vhostName], [role], [recoverFunc], [recoverFuncArgs])
- role: "Producer" or "Consumer"
- recoverFunc: When amqp reconnect will execute the function.
- recoverFuncArgs: recoverFunc's args

### closeConn([vhostName], [role])
- role: "Producer" or "Consumer"

## Licence

[MIT](LICENSE)
