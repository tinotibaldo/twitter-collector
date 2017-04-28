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
const collectionName = process.argv[2];
const gender = require('gender-guess');
const tweetsToProcess = 10;

const extractSourceDevice = (tweet) => {
    const devices = ["iphone", "android", "windows", "web"];
    const tweetSource = tweet.source.toLowerCase();
    let extractedDevice;
    devices.some((device) => {
        if (tweetSource.includes(device)) {
            extractedDevice = device;
            return true;
        }
    });
    return extractedDevice;
};

const extractUserGender = (tweet) => {
    const gender_guess = gender.guess(tweet.user.name);
    let userGender = 'unknown';
    if (gender_guess.confidence >= 0.5) {
        if (gender_guess.gender === 'M') {
            userGender = 'male';
        }
        if (gender_guess.gender === 'F') {
            userGender = 'female';
        }
    }
    return userGender;
};

dbConnection(process.env.mongodb_url)
	.then((db) => {
		const stream = twitterClient.stream('statuses/filter', params);
		let document;
		let count = 0;
		stream.on('data', (tweet) => {
			async.waterfall([
				(callback) => {
					const document = {
						text: tweet.text,
						user: {
							name: tweet.user.name,
							screen_name: tweet.user.screen_name,
							description: tweet.user.description,
							lang: tweet.user.lang,
							location: tweet.user.location,
							gender: extractUserGender(tweet),
							device: extractSourceDevice(tweet)
						},
						created_at: tweet.created_at,
						timestamp_ms: tweet.timestamp_ms
					};
					callback(null, document);
				},
				(document, callback) => {
					const collection = db.collection(`tweets_${collectionName}`)
					async.series({
						insert: (callback) => {
							collection.insertOne(document, (error, result) => {
								callback();
							});
						},
						count: (callback) => {
							collection.count((err, dbCount) => {
								console.log(`Tweets inserted in DB: ${dbCount}.`);
								callback(null, dbCount);
							});
						}
					});
					callback();
				}
			], (error, results) => {
				if (++count > tweetsToProcess) {
					console.log(`Process finished: Tweets inserted on this run: ${count}.\nKeyword/s: "${process.argv[2]}".`);
					process.exit();
				}
			});

		});
		stream.on("error", (error) => {
			throw error;
		});
	});