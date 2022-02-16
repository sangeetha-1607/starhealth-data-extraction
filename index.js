var MongoClient = require('mongodb').MongoClient;
var util = require('util');
var encoder = new util.TextDecoder('utf-8');
// const uri = 'mongodb+srv://readonly:IGucCZ6VULeTv1j2@securra-connect-dev.n9fin.mongodb.net/test?authSource=admin&replicaSet=securra-connect-dev-shard-0&readPreference=primary&ssl=true';
// const uri = 'mongodb://starhealth:STarHEalTH09346@docdb-2021-09-16-07-12-37.cmmxdqgb2co0.ap-south-1.docdb.amazonaws.com:27017/star-health-dev?tls=true&readPreference=secondaryPreferred&retryWrites=false';
//const uri = 'mongodb://appAdmin:HLASr!E*66Xm@wellness-cmp-db.cluster-c1sodybcij48.ap-south-1.docdb.amazonaws.com:27017/cmp-prod?tls=false&readPreference=secondaryPreferred&retryWrites=false';

const uri = 'mongodb://appAdmin:HLASr!E*66Xm@wellness-cmp-db.cluster-c1sodybcij48.ap-south-1.docdb.amazonaws.com:27017/cmp-prod?tls=false&authSource=admin&retryWrites=false';

(async ()=>{
    try {
        console.log("Connecting to DB")
        var fs = require('fs');
        const mongoDB = require('mongodb').ObjectID;
    
        let databaseName = "cmp-prod";
        const client = new MongoClient(uri, {
            tlsCAFile: `./rds-combined-ca-bundle.pem`,
	    useUnifiedTopology: true 
          })
    
        await client.connect();
        console.log("MongoDB Connected")
        const database = client.db(databaseName);
        const usersModel = database.collection("users");
        const labBatchReportModel = database.collection("lab-batch-report");
    
        const users = await usersModel.find({}).project({password:0}).toArray();
        const batchLabData = await labBatchReportModel.aggregate([
          {
            $lookup: {
              from: "lab-data",
              localField: "Identifier",
              foreignField: "identifier",
              as: "labData",
            },
          },
        ]).toArray();
        const batchLabDataMap = batchLabData.reduce(
          (acc, item) =>
            Object.assign(acc, {
              [String(item.PatientId)]: acc[String(item.PatientId)]
                ? [...acc[String(item.PatientId)], item]
                : [item],
            }),
          {}
        );
        const finalUser = users.reduce(
          (acc, item) => [
            ...acc,
            Object.assign(item, {
              batches: batchLabDataMap[String(item._id)] || [],
            }),
          ],
          []
        );
        let finalUserData = JSON.stringify(finalUser, null, 2);
        fs.writeFile(`user_lab_data_${new Date().getTime()}.json`, finalUserData, (err) => {
            if (err) throw err;
            console.log('Data written to file');
            process.exit(1);
        });    
        
    } catch (error) {
        console.log("error", error);
        process.exit(1);
    }

})();
