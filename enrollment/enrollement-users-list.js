var MongoClient = require('mongodb').MongoClient;
var ObjectID = require('mongodb').ObjectId;
const path = require("path");
const XLSX = require('xlsx');
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
            const usersModel  = database.collection("users");
            const administratorsModel  = database.collection("administrators");
            const doctorsModel  = database.collection("doctors");
            const ahpsModel  = database.collection("ahps");
            const chatUserModel  = database.collection("ahps");
            
            const chatMessagesAgg = [
              {
                $lookup: {
                  from: "chat-room-participants",
                  localField: "_id",
                  foreignField: "participant",
                  as: "chatRoomParticipants",
                },
              },
              {
                $unwind: {
                  path: "$chatRoomParticipants",
                  preserveNullAndEmptyArrays: false,
                },
              },
              {
                $lookup: {
                  from: "chat-room-messages",
                  localField: "chatRoomParticipants._id",
                  foreignField: "sender",
                  as: "chatRoomParticipants.messages",
                },
              },
              {
                $unwind: {
                  path: "$chatRoomParticipants.messages",
                  preserveNullAndEmptyArrays: false,
                },
              },
              {
                $group: {
                  _id: "$id",
                  count: { $sum: 1 },
                },
              },
            ];

            const cpAgg = [
              {
                $lookup: {
                  from: "user-care-programme-plans",
                  localField: "_id",
                  foreignField: "user",
                  as: "userCareProgramPlan",
                },
              },
              {
                $unwind: {
                  path: "$userCareProgramPlan",
                  preserveNullAndEmptyArrays: true,
                },
              },
              {
                $lookup: {
                  from: "care-programme-plans",
                  localField: "userCareProgramPlan.careProgrammePlan",
                  foreignField: "_id",
                  as: "userCareProgramPlan.careProgrammePlan",
                },
              },
              {
                $unwind: {
                  path: "$userCareProgramPlan.careProgrammePlan",
                  preserveNullAndEmptyArrays: true,
                },
              },
              {
                $lookup: {
                  from: "care-programmes",
                  localField: "userCareProgramPlan.careProgrammePlan.careProgramme",
                  foreignField: "_id",
                  as: "userCareProgramPlan.careProgrammePlan.careProgramme",
                },
              },
              {
                $unwind: {
                  path: "$userCareProgramPlan.careProgrammePlan.careProgramme",
                  preserveNullAndEmptyArrays: true,
                },
              }
            ];
            
            const [userCareprogramsplans, chatUsers] = await Promise.all([usersModel.aggregate(cpAgg).toArray(), chatUserModel.aggregate(chatMessagesAgg).toArray()])
            let chatUsersMap = chatUsers.reduce(
              (a, i) => Object.assign(a, { [String(i._id)]: i }),
              {}
            );
            const patients = userCareprogramsplans.map(async (item)=>{
                const careProgram = item.userCareProgramPlan && item.userCareProgramPlan.careProgrammePlan && item.userCareProgramPlan.careProgrammePlan.careProgramme
                const user = item
                const dobDate = user.dob && new Date(user.dob);
                const dob = dobDate && dobDate.getDate()+"-"+(dobDate.getMonth()+1)+"-"+dobDate.getFullYear();
                const enrollmentDate = item.userCareProgramPlan.createdAt && new Date(item.userCareProgramPlan.createdAt)
                const enrllDate = enrollmentDate && enrollmentDate.getDate()+"-"+(enrollmentDate.getMonth()+1)+"-"+enrollmentDate.getFullYear();
                
                const {role: approvedByRole, reference:approvedByReference} = item && item.userCareProgramPlan && item.userCareProgramPlan.approvedBy && item.userCareProgramPlan.approvedBy || {}
                
                let approvedBy;
                if(approvedByRole === 'administrator'){
                  approvedBy = await administratorsModel.findOne({_id: ObjectID(approvedByReference)})
                }
                else if(approvedByRole === 'doctor'){
                  approvedBy = await doctorsModel.findOne({_id: ObjectID(approvedByReference)})
                }
                else if(approvedByRole === 'ahp'){
                  let ahp = await ahpsModel.aggregate([
                    { $match: { _id: ObjectID(approvedByReference) } },
                    {
                      $lookup: {
                        from: "ahp-profiles",
                        localField: "AhpProfile",
                        foreignField: "_id",
                        as: "AhpProfile",
                      },
                    },
                    {
                      $unwind: {
                        path: "$AhpProfile",
                        preserveNullAndEmptyArrays: true,
                      },
                    }
                  ]);
                  approvedBy = ahp.AhpProfile
                }
                let userObject = {
                    firstName: user && user.name.first || "-",
                    lastName: user && user.name.last || "-",
                    mobile: user && user.mobile || "-",
                    email: user && user.email || "-",
                    dob: user && dob || "-",
                    careProgramName: careProgram && careProgram.name || "-",
                    enrolledRequestDate: enrllDate || "-",
                    enrolmentStatus: item && item.userCareProgramPlan && item.userCareProgramPlan.state || "-",
                    approvedBy: approvedBy && approvedBy.name && `${approvedBy.name.first} ${approvedBy.name.last}` || "-",
                    chatsCount: chatUsersMap[user._id] && chatUsersMap[user._id].count || "-"
                };
                console.log("userObject", userObject)
                return userObject
            })

            console.log("patients", patients[10])
            const workbook = XLSX.utils.book_new();
            var worksheet = XLSX.utils.json_to_sheet(patients, {
              header: Object.keys(patients[0]),
            });
            XLSX.utils.book_append_sheet(workbook, worksheet);
   
            let currTime = new Date()
            let dirName = currTime.toISOString().split("T").join("-").split(":").join("-").split(".")[0]
            // fs.mkdirSync(path.join(__dirname, dirName));
            // console.log('Dir ectory created successfully!', dirName);
            // XLSX.writeFile(workbook, path.resolve(__dirname, dirName, `user-enrolment-list-xlsx-${new Date().getTime()}.xlsx`))
            // fs.writeFileSync(path.resolve(__dirname, dirName, `user-enrolment-list-json-${new Date().getTime()}.json`), JSON.stringify(patients, null, 2));
            fs.writeFileSync(`${dirName}/unique-user-list-${new Date().getTime()}.json`, JSON.stringify(patients, null, 2));
            // fs.writeFileSync(`unique-user-list-${new Date().getTime()}.json`, JSON.stringify(patients, null, 2));
            process.exit(0);
    
            
    
    } catch (e) {
        console.error(e);
        process.exit(0);
    } finally {
        // await client.close();
        process.exit(0);
    }
}
    
main().catch((e)=>{
  console.error(e);
  process.exit(0);
});