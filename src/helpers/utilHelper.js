const _ = require('lodash')

exports.objArgsCheck = (obj = {}, args) => {
    const lackArgs = _.filter(args, arg => !obj[arg])
    if (lackArgs.length) {
        throw new Error(`needs：${lackArgs.join('、')}`)
    }
}
