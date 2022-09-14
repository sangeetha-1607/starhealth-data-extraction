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
            const usersModel  = database.collection("users");
            const userCareProgrammePlansModel  = database.collection("user-care-programme-plans");
            const userLockersModel  = database.collection("user-lockers");
            const administratorsModel  = database.collection("administrators");
            const doctorsModel  = database.collection("doctors");
            const ahpsModel  = database.collection("ahps");
            const chatUserModel  = database.collection("chat-users");
            
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

            const ucpDateAgg = [
              {
              $match: { "state": "active" }
              },
              {
                $sort: { "startDate": -1 }
              },
              {
                $group:{
                  _id: "$user",
                  createdAt: {$last: "$createdAt"},
                  startDate: {$last: "$startDate"}
                }
              }
            ];
            const requestRaisedAgg = [
              {
              $match: { "state": "enrollment_requested" }
              },
              {
                $group:{
                  _id: "$user",
                  count: { $sum: 1 }
                }
              }
            ];
            const uploadsCountAgg = [
              {
                $group:{
                  _id: "$user",
                  count: { $sum: 1 }
                }
              }
            ];

            

            const [userCareprogramsplans, userCareProgramDate, chatUsers, reqRaised, userUploads] = await Promise.all([
              usersModel.find().toArray(), 
              userCareProgrammePlansModel.aggregate(ucpDateAgg, {allowDiskUse: true}).toArray(), 
              chatUserModel.aggregate(chatMessagesAgg).toArray(),
              userCareProgrammePlansModel.aggregate(requestRaisedAgg).toArray(),
              userLockersModel.aggregate(uploadsCountAgg).toArray(),
            ])
            let chatUsersMap = chatUsers.reduce(
              (a, i) => Object.assign(a, { [String(i._id)]: i }),
              {}
            );
            let userCareProgramDateMap = userCareProgramDate.reduce(
              (a, i) => Object.assign(a, { [String(i._id)]: i }),
              {}
            );
            let reqRaisedMap = reqRaised.reduce(
              (a, i) => Object.assign(a, { [String(i._id)]: i }),
              {}
            );
            let userUploadsMap = userUploads.reduce(
              (a, i) => Object.assign(a, { [String(i._id)]: i }),
              {}
            );
            const patients = await Promise.all(userCareprogramsplans.map(async (user)=>{
                const currDate = new Date()
                const dobDate = user.dob && new Date(user.dob);
                const age = dobDate && currDate.getFullYear()-dobDate.getFullYear();
                const enrollmentDate = userCareProgramDateMap[String(user._id)] && userCareProgramDateMap[String(user._id)].createdAt && new Date(userCareProgramDateMap[String(user._id)].createdAt)
                const enrllDate = enrollmentDate && enrollmentDate.getDate()+"-"+(enrollmentDate.getMonth()+1)+"-"+enrollmentDate.getFullYear();
                const startDate = userCareProgramDateMap[String(user._id)] && userCareProgramDateMap[String(user._id)].startDate && new Date(userCareProgramDateMap[String(user._id)].startDate)
                const formattedStartDate = startDate && startDate.getDate()+"-"+(startDate.getMonth()+1)+"-"+startDate.getFullYear();
                
                let userObject = {
                    firstName: user && user.name.first || "-",
                    lastName: user && user.name.last || "-",
                    mobile: user && user.mobile || "-",
                    email: user && user.email || "-",
                    dob: user && user.dob || "-",
                    age: user && user.dob && age || "-",
                    location: user.addresses.map(item=>item.city).join(",") || "-",
                    requestRaisedDate: enrllDate || "-",
                    programAdmissionDate: formattedStartDate || "-",
                    signupDate: user && user.createdAt,
                    CTAUploadsCount: userUploadsMap[String(user._id)] && userUploadsMap[String(user._id)].count || "-",
                    CTARequestRaisedCount: reqRaisedMap[String(user._id)] && reqRaisedMap[String(user._id)].count || "-",
                    CTAChatsCount: chatUsersMap[String(user._id)] && chatUsersMap[String(user._id)].count || "-",
                    deviceIDs: user.mobileDevices.map(item=>item.deviceId).join(",") || "-"
                };
                return userObject
            }))

            const workbook = XLSX.utils.book_new();
            var worksheet = XLSX.utils.json_to_sheet(patients, {
              header: Object.keys(patients[0]),
            });
            XLSX.utils.book_append_sheet(workbook, worksheet);
   
            let currTime = new Date()
            let dirName = currTime.toISOString().split("T").join("-").split(":").join("-").split(".")[0]
            fs.mkdirSync(path.join(__dirname, dirName));
            console.log('Directory created successfully!', dirName);
            XLSX.writeFile(workbook, path.resolve(__dirname, dirName, `user-enrolment-list-xlsx-${new Date().getTime()}.xlsx`))
            fs.writeFileSync(path.resolve(__dirname, dirName, `user-enrolment-list-json-${new Date().getTime()}.json`), JSON.stringify(patients, null, 2));
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