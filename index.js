if (!process.argv[2]) {
	console.log("You must provide an argument.", "\nProcess finished.");
	process.exit();
} else {
	console.log(`Argument provided: ${process.argv[2]}`);
}

const async = require('async');
const dbConnection = require('./db_connection');
const Twitter = require('twitter');
const twitterClient = new Twitter({
        consumer_key: process.env.twitter_consumer_key,
        consumer_secret: process.env.twitter_consumer_secret,
        access_token_key: process.env.twitter_access_token_key,
        access_token_secret: process.env.twitter_access_token_secret
    });
const params = { track: process.argv[2] };

dbConnection(process.env.mongodb_url)
	.then((db) => {
		const stream = twitterClient.stream('statuses/filter', params);
		let document;
		let count = 0;
		stream.on('data', (tweet) => {
			if (!tweet.text.startsWith('RT')) {
				async.waterfall([
					(callback) => {
						const document = {
							text: tweet.text,
							user: {
								name: tweet.user.name,
								screen_name: tweet.user.screen_name,
								description: tweet.user.description,
								lang: tweet.user.lang,
								location: tweet.user.location
							},
							created_at: tweet.created_at,
							timestamp_ms: tweet.timestamp_ms
						};
						callback(null, document);
					},
					(document, callback) => {
						async.series({
							insert: (callback) => {
								db.collection('tweets').insertOne(document, (error, result) => {
									callback();
								});
							},
							count: (callback) => {
								db.collection('tweets').count((err, dbCount) => {
									console.log(`Tweets inserted in DB: ${dbCount}.`);
									callback(null, dbCount);
								});
							}
						});
						callback();
					}
				], (error, results) => {
					if (++count > config.tweetsToProcess) {
						console.log(`Process finished: Tweets inserted on this run: ${config.tweetsToProcess}.\nKeyword/s: "${process.argv[2]}".`);
						process.exit();
					}
				});

			}
		});
		stream.on("error", (error) => {
			throw error;
		});
	});