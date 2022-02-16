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
        const careProgrammePlansModel = database.collection("care-programme-plans");
        const userCareProgrammePlansModel = database.collection("user-care-programme-plans");
        const careProgrammeData = await careProgrammeModel.find({}).project({_id:1, participants:1}).toArray();
        const careProgramMap = careProgrammeData.reduce((acc, item) => Object.assign(acc, {[String(item._id)]:item}),{})
        const careProgramIds = careProgrammeData.map(item=>item._id)
        const careProgrammePlanData = await careProgrammePlansModel.find({"careProgramme":{$in: careProgramIds}}).project({_id:1, careProgramme:1}).toArray();
        const careProgrammePlanMap = careProgrammePlanData.reduce((acc, item) => Object.assign(acc, {[String(item._id)]: Object.assign(item, {careProgramme: careProgramMap[String(item.careProgramme)]})}),{})
        const careProgrammePlanIds = careProgrammePlanData.map(item=>item._id)
        const userCareProgrammePlansData = await userCareProgrammePlansModel.find({"careProgrammePlan":{$in: careProgrammePlanIds}}).project({_id:1, participants:1, careProgrammePlan:1}).toArray();

        let userCareProgrammePlansBulkWriteOperations = userCareProgrammePlansData.reduce((acc, item) => {
          const participants = careProgrammePlanMap[String(item.careProgrammePlan)]?.careProgramme?.participants
          const doctor = item.participants?.find(
            (participant) =>
              participant.role === "doctor" && participant.isPrimary === true
          );
          if (!doctor) {
            const careProgramDoctor = participants?.find(
              (participant) =>
                participant.role === "doctor" && participant.isPrimary === true
            );
            return [...acc, {
              updateOne: {
                filter: { _id: item._id },
                update: { $push: { participants: careProgramDoctor } }
              }
            }];
          }
          return acc;
        },[]);
        await userCareProgrammePlansModel.bulkWrite(userCareProgrammePlansBulkWriteOperations);
        console.log('User Care Program data modified successfully');
        process.exit(1);
        
    } catch (error) {
        console.log("error", error);
        process.exit(1);
    }

})();


