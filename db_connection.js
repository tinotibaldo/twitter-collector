const MongoClient = require('mongodb').MongoClient;

module.exports = (url) => {
    if (!url) {
        return
    };
    return new Promise((resolve, reject) => {
        MongoClient
            .connect(url)
            .then((db) => {
                if (db) {
                    resolve(db);
                } else {
                    reject('Connection to DB failed.');
                }
            });
    });
}
