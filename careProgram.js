var MongoClient = require('mongodb').MongoClient;
var util = require('util');
// const uri = 'mongodb+srv://readonly:IGucCZ6VULeTv1j2@securra-connect-dev.n9fin.mongodb.net/test?authSource=admin&replicaSet=securra-connect-dev-shard-0&readPreference=primary&ssl=true';
// const uri = 'mongodb://starhealth:STarHEalTH09346@docdb-2021-09-16-07-12-37.cmmxdqgb2co0.ap-south-1.docdb.amazonaws.com:27017/star-health-dev?tls=true&readPreference=secondaryPreferred&retryWrites=false';
//const uri = 'mongodb://appAdmin:HLASr!E*66Xm@wellness-cmp-db.cluster-c1sodybcij48.ap-south-1.docdb.amazonaws.com:27017/cmp-prod?tls=false&readPreference=secondaryPreferred&retryWrites=false';

const uri = 'mongodb://appAdmin:HLASr!E*66Xm@wellness-cmp-db.cluster-c1sodybcij48.ap-south-1.docdb.amazonaws.com:27017/cmp-prod?tls=false&authSource=admin&retryWrites=false';
// const uri = 'mongodb://localhost:27018';

(async ()=>{
    try {
        console.log("Connecting to DB")
        const {ObjectId} = require('mongodb')
    
        // let databaseName = "star-prod";
        let databaseName = "cmp-prod";
        const client = new MongoClient(uri, {
          tlsCAFile: `./rds-combined-ca-bundle.pem`,
	        useUnifiedTopology: true 
        })
    
        await client.connect();
        console.log("MongoDB Connected")
        const database = client.db(databaseName);
        const careProgrammeModel = database.collection("care-programmes");

        const careProgrammeData = await careProgrammeModel.find({}).toArray();
        let careProgrammeBulkWriteOperations = careProgrammeData.reduce((acc, item) => {
          const doctor = item.participants.find(
            (participant) =>
              participant?.role === "doctor" && participant?.isPrimary === true
          );
          if (!doctor) {
            let participantObj = {
              isPrimary: true,
              isPrimaryPhysician: false,
              _id: new ObjectId(),
              bandwidth: 100,
              reference: item.doctor,
              role: "doctor",
            };
            return [...acc, {
              updateOne: {
                filter: { _id: item._id },
                update: { $push: { participants: participantObj } }
              }
            }];
          }
          return acc;
        },[]);
        await careProgrammeModel.bulkWrite(careProgrammeBulkWriteOperations);
        console.log('Care Program data modified successfully');
        process.exit(1);
        
    } catch (error) {
        console.log("error", error);
        process.exit(1);
    }

})();


