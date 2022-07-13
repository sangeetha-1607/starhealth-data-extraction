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
            const careProgrammeQuestionsModel  = database.collection("care-programme-questions");
            const usersModel  = database.collection("users");
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
              },
              {
                $unwind: {
                  path: "$userCareProgramPlan.screeningQuestions",
                  preserveNullAndEmptyArrays: true,
                },
              },
              {
                $lookup: {
                  from: "care-programme-questions",
                  localField: "userCareProgramPlan.screeningQuestions.careProgrammeQuestion",
                  foreignField: "_id",
                  as: "userCareProgramPlan.screeningQuestions.careProgrammeQuestion",
                },
              },
              {
                $unwind: {
                  path: "$userCareProgramPlan.screeningQuestions.careProgrammeQuestion",
                  preserveNullAndEmptyArrays: true,
                },
              },
              {
                $unwind: {
                  path: "$onboardingQuestions",
                  preserveNullAndEmptyArrays: true,
                },
              },
              {
                $lookup: {
                  from: "onboarding-questions",
                  localField: "onboardingQuestions.onboardingQuestion",
                  foreignField: "_id",
                  as: "onboardingQuestions.onboardingQuestion",
                },
              },
              {
                $unwind: {
                  path: "$onboardingQuestions.onboardingQuestion",
                  preserveNullAndEmptyArrays: true,
                },
              },
              {
                $group:{
                  _id: "$_id",
                  userData: {$first: "$$ROOT"},
                  screeningQuestions: {$addToSet: "$userCareProgramPlan.screeningQuestions"},
                  onboardingQuestions: {$addToSet: "$onboardingQuestions.onboardingQuestion"}
                }
              },
            ];

            

            const onboardingQuestionsModel  = database.collection("onboarding-questions");

            const [userCareprogramsplans, chatUsers, careProgrammeQuestions, onboardingQuestions] = await Promise.all([
              usersModel.aggregate(cpAgg, {allowDiskUse: true}).toArray(), 
              chatUserModel.aggregate(chatMessagesAgg).toArray(),
              careProgrammeQuestionsModel.find().toArray(),
              onboardingQuestionsModel.find({ status: "active" }).toArray()
            ])
            let chatUsersMap = chatUsers.reduce(
              (a, i) => Object.assign(a, { [String(i._id)]: i }),
              {}
            );
            const patients = await Promise.all(userCareprogramsplans.map(async (item)=>{
                const careProgram = item.userData.userCareProgramPlan && item.userData.userCareProgramPlan.careProgrammePlan && item.userData.userCareProgramPlan.careProgrammePlan.careProgramme
                const user = item.userData
                const dobDate = user.dob && new Date(user.dob);
                const dob = dobDate && dobDate.getDate()+"-"+(dobDate.getMonth()+1)+"-"+dobDate.getFullYear();
                const enrollmentDate = item.userData.userCareProgramPlan.createdAt && new Date(item.userData.userCareProgramPlan.createdAt)
                const enrllDate = enrollmentDate && enrollmentDate.getDate()+"-"+(enrollmentDate.getMonth()+1)+"-"+enrollmentDate.getFullYear();
                const startDate = item.userData.userCareProgramPlan.startDate && new Date(item.userData.userCareProgramPlan.startDate)
                const formattedStartDate = startDate && startDate.getDate()+"-"+(startDate.getMonth()+1)+"-"+startDate.getFullYear();
                
                let answeredScreeningQuestionsCount = item.screeningQuestions.filter((item) => {
                  return (
                    item.careProgrammeQuestion &&
                    item.careProgrammeQuestion.status === "active"
                  );
                });
                const screeningQuestionCompletedPercentage = careProgrammeQuestions.length > 0 ? Math.floor( (answeredScreeningQuestionsCount.length / careProgrammeQuestions.length) * 100 ) : 0;

                let answeredOnboardingQuestionsCount = item.onboardingQuestions.filter((item) => {
                  return (
                    item.onboardingQuestion &&
                    item.onboardingQuestion.status === "active"
                  );
                });

                const onboardingQuestionCompletionPercentage = onboardingQuestions.length > 0 ? Math.floor( (answeredOnboardingQuestionsCount.length / onboardingQuestions.length) * 100 ) : 0;

                const {role: approvedByRole, reference:approvedByReference} = item && item.userData.userCareProgramPlan && item.userData.userCareProgramPlan.approvedBy && item.userData.userCareProgramPlan.approvedBy || {}
                
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
                    startDate: formattedStartDate || "-",
                    chatsCount: chatUsersMap[String(user._id)] && chatUsersMap[String(user._id)].count || "-",
                    screeningQuestionPercentage: screeningQuestionCompletedPercentage+" %" || "0%",
                    onboardingQuestionPercentage: onboardingQuestionCompletionPercentage+" %" || "0%"
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