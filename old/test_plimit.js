const pLimit = require('p-limit');
console.log('pLimit type:', typeof pLimit);
const limit = pLimit(5);
console.log('limit created:', typeof limit === 'function');