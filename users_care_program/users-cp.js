var MongoClient = require('mongodb').MongoClient;
const fs = require('fs');

async function main(){
    const uri = "mongodb://appAdmin:HLASr!E*66Xm@wellness-cmp-db.cluster-c1sodybcij48.ap-south-1.docdb.amazonaws.com:27017/cmp-prod?tls=false&authSource=admin&retryWrites=false";
    // const client = new MongoClient(uri, {  useNewUrlParser: true, useUnifiedTopology: true } );
    try {
            console.log("Connecting to DB")


            const client = new MongoClient(uri, {
              tlsCAFile: `../rds-combined-ca-bundle.pem`,
              useUnifiedTopology: true 
              })
            await client.connect();
            console.log("MongoDB connected!!!")
            const database = client.db("cmp-prod");
            const careProgrammeModel  = database.collection("care-programmes");
            
            const cpAgg = [
              {
                $match: {
                  name: { $regex: "^Condition Management Program$" },
                },
              },
              {
                $lookup: {
                  from: "care-programme-plans",
                  localField: "_id",
                  foreignField: "careProgramme",
                  as: "careProgrammePlan",
                },
              },
              {
                $unwind: {
                  path: "$careProgrammePlan",
                  preserveNullAndEmptyArrays: false,
                },
              },
              {
                $lookup: {
                  from: "user-care-programme-plans",
                  localField: "careProgrammePlan._id",
                  foreignField: "careProgrammePlan",
                  as: "careProgrammePlan.userCareProgramPlan",
                },
              },
              {
                $unwind: {
                  path: "$careProgrammePlan.userCareProgramPlan",
                  preserveNullAndEmptyArrays: false,
                },
              },
              {
                $lookup: {
                  from: "users",
                  localField: "careProgrammePlan.userCareProgramPlan.user",
                  foreignField: "_id",
                  as: "careProgrammePlan.userCareProgramPlan.user",
                },
              },
              {
                $unwind: {
                  path: "$careProgrammePlan.userCareProgramPlan.user",
                  preserveNullAndEmptyArrays: false,
                },
              }
            ];
            const userCareprogramsplans = await careProgrammeModel.aggregate(cpAgg).toArray()


            const patients = userCareprogramsplans.map((item)=>{
                const user = item.careProgrammePlan && item.careProgrammePlan.userCareProgramPlan && item.careProgrammePlan.userCareProgramPlan.user
                const dobDate = user.dob && new Date(user.dob);
                const dob = dobDate && dobDate.getDate()+"-"+(dobDate.getMonth()+1)+"-"+dobDate.getFullYear();
                const enrollmentDate = item.careProgrammePlan.userCareProgramPlan.createdAt && new Date(item.careProgrammePlan.userCareProgramPlan.createdAt)
                const enrllDate = enrollmentDate && enrollmentDate.getDate()+"-"+(enrollmentDate.getMonth()+1)+"-"+enrollmentDate.getFullYear();
                let userObject = {
                    firstName: user && user.name.first || "-",
                    lastName: user && user.name.last || "-",
                    mobile: user && user.mobile || "-",
                    email: user && user.email || "-",
                    dob: user && dob || "-",
                    careProgramName: item.name || "-",
                    enrolledDate: enrllDate || "-",
                    status: item.careProgrammePlan.userCareProgramPlan.state || "-",
                    
                };
                return userObject
            })
            
            fs.writeFileSync(`users-care-program-${new Date().getTime()}.json`, JSON.stringify(patients, null, 2));
            process.exit(0);
    
    } catch (e) {
        console.error(e);
        process.exit(0);
    } finally {
        await client.close();
        process.exit(0);
    }
}
    
main().catch((e)=>{
  console.error(e);
  process.exit(0);
});