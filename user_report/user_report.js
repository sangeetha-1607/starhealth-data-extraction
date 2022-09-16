var MongoClient = require('mongodb').MongoClient;
var ObjectID = require('mongodb').ObjectId;
const path = require("path");
const XLSX = require('xlsx');
const fs = require('fs');

function formatDate(date){
  if(!date){
    return
  }
  let tempDate = new Date(date)
  return tempDate.getDate()+"-"+(tempDate.getMonth()+1)+"-"+tempDate.getFullYear();
}
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
                let userObject = {
                    FIRSTNAME: user && user.name.first || "-",
                    LASTNAME: user && user.name.last || "-",
                    MOBILE: user && user.mobile || "-",
                    EMAIL: user && user.email || "-",
                    DOB: user && user.dob && formatDate(user.dob) || "-",
                    AGE: user && user.dob && age || "-",
                    LOCATION: user.addresses.map(item=>item.city).join(",") || "-",
                    REQUESTRAISEDDATE: userCareProgramDateMap[String(user._id)] && userCareProgramDateMap[String(user._id)].createdAt && formatDate(userCareProgramDateMap[String(user._id)].createdAt) || "-",
                    PROGRAMADMISSIONDATE: userCareProgramDateMap[String(user._id)] && userCareProgramDateMap[String(user._id)].startDate && formatDate(userCareProgramDateMap[String(user._id)].startDate) || "-",
                    REGISTEREDDATE: user && user.createdAt,
                    CTAUPLOADSCOUNT: userUploadsMap[String(user._id)] && userUploadsMap[String(user._id)].count || "-",
                    CTAREQUESTRAISEDCOUNT: reqRaisedMap[String(user._id)] && reqRaisedMap[String(user._id)].count || "-",
                    CTACHATCOUNT: chatUsersMap[String(user._id)] && chatUsersMap[String(user._id)].count || "-"
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
            XLSX.writeFile(workbook, path.resolve(__dirname, dirName, `user-report-xlsx-${new Date().getTime()}.xlsx`))
            fs.writeFileSync(path.resolve(__dirname, dirName, `user-report-json-${new Date().getTime()}.json`), JSON.stringify(patients, null, 2));
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