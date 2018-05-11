use mydb

var notReTweets = db.coll.find( { retweeted_status : { $exists : false } }, {id:1, "retweeted_status.id":1, "retweeted_status.retweet_count":1,"favorite_count":1 ,"created_at":1} )
db.createCollection('collOriginal')
notReTweets.forEach(function(x){ db.collOriginal.insertOne(x) })

# update new collection with additional fields. 
db.collOriginal.find().snapshot().forEach(
  function (e) {
    e.orig_id = e.id;
    e.ret_count = 0;
    db.collOriginal.save(e);
  }
)

var reTweets = db.coll.find( { retweeted_status : { $exists : true } }, {id:1, "retweeted_status.id":1, "retweeted_status.retweet_count":1, "retweeted_status.favorite_count":1, "created_at":1} )

# Add re tweets to the new collection
reTweets.forEach(function(x){ db.collOriginal.insertOne({'id': x.id, 'orig_id': x.retweeted_status.id,'ret_count':x.retweeted_status.retweet_count ,'favorite_count':x.retweeted_status.favorite_count,'created_at':x.created_at}) })



var arr = db.collOriginal.group(
    {key: {orig_id:true},
        reduce: function(obj,prev) { 
            if (prev.latest_ret_count < obj.ret_count) { 
                prev.latest_ret_count = obj.ret_count; 
                prev.maxId = obj.id
                prev.latest_date = obj.created_at
                prev.latest_fav_count = obj.favorite_count
            }  
        },
    initial: { latest_ret_count: -1, maxId: -1, latest_date:""}}
);

db.createCollection('temp')
db.temp.insert(arr)


# Query 1 - Find the latest date of each tweet. 
db.temp.find({}, {orig_id:1, latest_date :1 })

# Query 2 - Find out the retweet count and favourite count for the latest version of each tweet.

db.temp.find({}, {orig_id:1, latest_ret_count:1, latest_fav_count:1 })

# Returns the id of the tweet that was retweeted the most - query 3a
db.collOriginal.find().sort({ret_count:-1}).limit(1)

# Returns the id of the tweet that has highest fav count - query 3b
db.collOriginal.find().sort({favorite_count:-1}).limit(1)




