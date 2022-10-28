var MongoClient = require('mongodb').MongoClient;
var ObjectID = require('mongodb').ObjectId;
const path = require("path");
const XLSX = require('xlsx');
const fs = require('fs');

async function main(){
    // const uri = "mongodb://appAdmin:HLASr!E*66Xm@wellness-cmp-db.cluster-c1sodybcij48.ap-south-1.docdb.amazonaws.com:27017/cmp-prod?tls=false&authSource=admin&retryWrites=false";
    const uri = "mongodb://localhost:27018";
    // const client = new MongoClient(uri, {  useNewUrlParser: true, useUnifiedTopology: true } );
    try {
            console.log("Connecting to DB")


            const client = new MongoClient(uri, {
              // tlsCAFile: `../rds-combined-ca-bundle.pem`,
              useUnifiedTopology: true 
              })
            await client.connect();
            console.log("MongoDB connected!!!")
            // const database = client.db("cmp-prod");
            const database = client.db("star-health-dev");
            const careProgrammeQuestionsModel  = database.collection("care-programme-questions");
            const usersModel  = database.collection("users");
            const administratorsModel  = database.collection("administrators");
            const doctorsModel  = database.collection("doctors");
            const ahpsModel  = database.collection("ahps");
            
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
                  preserveNullAndEmptyArrays: false,
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

            const obqAgg = [
              {
                $lookup: {
                  from: "questions",
                  localField: "question",
                  foreignField: "_id",
                  as: "question",
                },
              },
              {
                $unwind: {
                  path: "$question",
                  preserveNullAndEmptyArrays: false,
                },
              },
              {
                $match: {
                    "status": "active"
                }
              }
            ];
            
            const onboardingQuestionsModel  = database.collection("onboarding-questions");

            const [userCareprogramsplans, careProgrammeQuestions, onboardingQuestions] = await Promise.all([
              usersModel.aggregate(cpAgg, {allowDiskUse: true}).toArray(), 
              careProgrammeQuestionsModel.find().toArray(),
              onboardingQuestionsModel.aggregate(obqAgg).toArray()
            ])
            let questionNamesMap = {}
            
            const onboardingQuestionMap = onboardingQuestions.reduce((acc, item)=>{
                acc[String(item._id)] = JSON.parse(JSON.stringify(item));
                questionNamesMap[String(item.question.title.toLowerCase().split(" ").join("_"))]={}
                return acc; 
            },{});
            const patients = await Promise.all(userCareprogramsplans.map(async (item)=>{
                const careProgram = item.userData.userCareProgramPlan && item.userData.userCareProgramPlan.careProgrammePlan && item.userData.userCareProgramPlan.careProgrammePlan.careProgramme
                const user = item.userData
                const dobDate = user.dob && new Date(user.dob);
                const dob = dobDate && dobDate.getDate()+"-"+(dobDate.getMonth()+1)+"-"+dobDate.getFullYear();
                const enrollmentDate = item.userData.userCareProgramPlan.createdAt && new Date(item.userData.userCareProgramPlan.createdAt)
                const enrllDate = enrollmentDate && enrollmentDate.getDate()+"-"+(enrollmentDate.getMonth()+1)+"-"+enrollmentDate.getFullYear();
                const startDate = item.userData.userCareProgramPlan.startDate && new Date(item.userData.userCareProgramPlan.startDate)
                const formattedStartDate = startDate && startDate.getDate()+"-"+(startDate.getMonth()+1)+"-"+startDate.getFullYear();
                
                const currDate = new Date()
                // const dobDate = user.dob && new Date(user.dob);
                const age = dobDate && currDate.getFullYear()-dobDate.getFullYear();
                const [address] = user.addresses
                const height = user.medicalProfile && user.medicalProfile.height;
                const weight = user.medicalProfile && user.medicalProfile.weight ? user.medicalProfile.weight : user.medicalProfile && user.medicalProfile.recentVitals && user.medicalProfile.recentVitals.vital_body_weight;
                const bmi = (height && weight) && Number.parseFloat(Number.parseFloat((weight/(height*height))*10000).toFixed(2));
                
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

                let onboardingQues = Object.assign({}, questionNamesMap );

                item.onboardingQuestions.forEach(item=>{
                    let answer = item.answer;
                    if(onboardingQuestionMap[item.onboardingQuestion].question["type"] === "multiple-choice-multi-select"){
                        answer =onboardingQuestionMap[item.onboardingQuestion].question.options.filter(opItem=>item.answer.indexOf(String(opItem._id))>0).map(i=>i.value).join(", ")
                    }
                    if(onboardingQuestionMap[item.onboardingQuestion].question["type"] === "multiple-choice-single-select"){
                        answer =onboardingQuestionMap[item.onboardingQuestion].question.options.find(opItem=>String(item.answer) === String(opItem._id)).value
                    }
                    onboardingQues[String(onboardingQuestionMap[item.onboardingQuestion].question.title.toLowerCase().split(" ").join("_"))] = Object.assign({}, onboardingQuestionMap[item.onboardingQuestion], {answer})
                })

                let userObject = {
                    firstName: user && user.name.first || "-",
                    lastName: user && user.name.last || "-",
                    mobile: user && user.mobile || "-",
                    email: user && user.email || "-",
                    gender: user && user.gender || "-",
                    dob: user && dob || "-",
                    age: age > 0 ? age : "-",
                    ageClassification:
                        age > 0 && age <= 12
                        ? "Child"
                        : age >= 13 && age <= 18
                        ? "Adolescence"
                        : age >= 19 && age <= 59
                        ? "Adult"
                        : age >= 60
                        ? "Senior Adult"
                        : "-",
                    city: address && address.city || "-",
                    state: address && address.state || "-",
                    patientUHID: user._id || "-",
                    height: height || "-",
                    weight: weight || "-",
                    BMI: bmi || "-",
                    BMIClassification:
                        bmi < 18
                        ? "Under Weight"
                        : bmi >= 18.0 && bmi <= 22.4
                        ? "Normal"
                        : bmi >= 22.5 && bmi <= 27.4
                        ? "Overweight"
                        : bmi >= 27.5 && bmi <= 32.4
                        ? "Moderate Obese"
                        : bmi >= 32.5 && bmi <= 37.4
                        ? "Severe Obese"
                        : bmi >= 37.5 && bmi <= 44.4
                        ? "Morbidly Obese"
                        : bmi >= 45
                        ? "Super Obese"
                        : "-",
                    waistCircumference: user.medicalProfile && user.medicalProfile.recentVitals && user.medicalProfile.recentVitals.vital_waist_hip_ratio && user.medicalProfile.recentVitals.vital_waist_hip_ratio.waist || "-",
                    hipCircumference: user.medicalProfile && user.medicalProfile.recentVitals && user.medicalProfile.recentVitals.vital_waist_hip_ratio && user.medicalProfile.recentVitals.vital_waist_hip_ratio.hip || "-",
                    waist: user.medicalProfile && user.medicalProfile.recentVitals && user.medicalProfile.recentVitals.vital_waist_hip_ratio && user.medicalProfile.recentVitals.vital_waist_hip_ratio.value || "-",
                    BP: user.medicalProfile && user.medicalProfile.recentVitals && user.medicalProfile.recentVitals.vital_bp || "-",
                    CAREPROGRAMNAME: careProgram && careProgram.name || "-",
                    ENROLLEDREQUESTDATE: enrllDate || "-",
                    ENROLMENTSTATUS: item && item.userData && item.userData.userCareProgramPlan && item.userData.userCareProgramPlan.state || "-",
                    APPROVEDBY: approvedBy && approvedBy.name && `${approvedBy.name.first} ${approvedBy.name.last}` || "-",
                    APPROVEDDATE: formattedStartDate || "-",
                    REGISTEREDDATE: user && user.createdAt
                };
                Object.assign(userObject, onboardingQues)
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