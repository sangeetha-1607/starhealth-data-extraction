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
                $match: { userCareProgramPlan: {$exists: false}},
              }
            ];

            

            // const onboardingQuestionsModel  = database.collection("onboarding-questions");

            const [users, chatUsers] = await Promise.all([
              usersModel.aggregate(cpAgg, {allowDiskUse: true}).toArray(), 
              chatUserModel.aggregate(chatMessagesAgg).toArray()
            ])
            let chatUsersMap = chatUsers.reduce(
              (a, i) => Object.assign(a, { [String(i._id)]: i }),
              {}
            );
            const patients = await Promise.all(users.map(async (item)=>{
                const user = item
                const dobDate = user.dob && new Date(user.dob);
                const dob = dobDate && dobDate.getDate()+"-"+(dobDate.getMonth()+1)+"-"+dobDate.getFullYear();
                
                let userObject = {
                    firstName: user && user.name.first || "-",
                    lastName: user && user.name.last || "-",
                    mobile: user && user.mobile || "-",
                    email: user && user.email || "-",
                    dob: user && dob || "-",
                    chatsCount: chatUsersMap[String(user._id)] && chatUsersMap[String(user._id)].count || "-",
                    createdAt: user && user.createdAt
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
            XLSX.writeFile(workbook, path.resolve(__dirname, dirName, `user-unenrolment-list-xlsx-${new Date().getTime()}.xlsx`))
            fs.writeFileSync(path.resolve(__dirname, dirName, `user-unenrolment-list-json-${new Date().getTime()}.json`), JSON.stringify(patients, null, 2));
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