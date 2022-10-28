var MongoClient = require('mongodb').MongoClient;
var ObjectID = require('mongodb').ObjectId;
const path = require("path");
const XLSX = require('xlsx');
const fs = require('fs');

async function main(){
    const uri = "mongodb://appAdmin:HLASr!E*66Xm@wellness-cmp-db.cluster-c1sodybcij48.ap-south-1.docdb.amazonaws.com:27017/cmp-prod?tls=false&authSource=admin&retryWrites=false";
    // const uri = "mongodb://localhost:27018";
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
            // const database = client.db("star-health-dev");
            const careProgrammeQuestionsModel  = database.collection("care-programme-questions");
            const usersModel  = database.collection("users");
            const administratorsModel  = database.collection("administrators");
            const doctorsModel  = database.collection("doctors");
            const ahpsModel  = database.collection("ahps");
            
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

            const [userCareprogramsplans, onboardingQuestions] = await Promise.all([
              usersModel.aggregate(cpAgg, {allowDiskUse: true}).toArray(),
              onboardingQuestionsModel.aggregate(obqAgg).toArray()
            ])
            let questionNamesMap = {}
            
            const onboardingQuestionMap = onboardingQuestions.reduce((acc, item)=>{
                acc[String(item._id)] = JSON.parse(JSON.stringify(item));
                questionNamesMap[String(item.question.title.toLowerCase().split(" ").join("_"))]={}
                return acc; 
            },{});
            const patients = await Promise.all(userCareprogramsplans.map(async (item)=>{
                
                const careProgram = item.userCareProgramPlan && item.userCareProgramPlan.careProgrammePlan && item.userCareProgramPlan.careProgrammePlan.careProgramme
                const user = item
                const dobDate = user.dob && new Date(user.dob);
                const dob = dobDate && dobDate.getDate()+"-"+(dobDate.getMonth()+1)+"-"+dobDate.getFullYear();
                const enrollmentDate = item.userCareProgramPlan.createdAt && new Date(item.userCareProgramPlan.createdAt)
                const enrllDate = enrollmentDate && enrollmentDate.getDate()+"-"+(enrollmentDate.getMonth()+1)+"-"+enrollmentDate.getFullYear();
                const startDate = item.userCareProgramPlan.startDate && new Date(item.userCareProgramPlan.startDate)
                const formattedStartDate = startDate && startDate.getDate()+"-"+(startDate.getMonth()+1)+"-"+startDate.getFullYear();
                
                const currDate = new Date()
                const age = dobDate && currDate.getFullYear()-dobDate.getFullYear();
                const [address] = user.addresses
                const height = user.medicalProfile && user.medicalProfile.height;
                const weight = user.medicalProfile && user.medicalProfile.weight ? user.medicalProfile.weight : user.medicalProfile && user.medicalProfile.recentVitals && user.medicalProfile.recentVitals.vital_body_weight;
                const bmi = (height && weight) && Number.parseFloat(Number.parseFloat((weight/(height*height))*10000).toFixed(2));
                
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

                let onboardingQues = Object.assign({}, questionNamesMap );
                
                item.onboardingQuestions.forEach(obqitem=>{
                    let answer = obqitem.answer;
                    if(onboardingQuestionMap[obqitem.onboardingQuestion].question["type"] === "multiple-choice-multi-select"){
                        answer =onboardingQuestionMap[obqitem.onboardingQuestion].question.options.filter(opItem=>obqitem.answer.indexOf(String(opItem._id))>0).map(i=>i.value).join(", ")
                    }
                    if(onboardingQuestionMap[obqitem.onboardingQuestion].question["type"] === "multiple-choice-single-select"){
                        console.log("onboardingQuestionMap[obqitem.onboardingQuestion].question.options", onboardingQuestionMap[obqitem.onboardingQuestion].question.options)
                        console.log("obqitem.answer", obqitem.answer)
                        console.log("item.name, id", item._id, item.name)
                        answer =onboardingQuestionMap[obqitem.onboardingQuestion].question.options.find(opItem=>{
                          return String(obqitem.answer) === String(opItem._id)
                        }).value
                    }
                    onboardingQues[String(onboardingQuestionMap[obqitem.onboardingQuestion].question.title.toLowerCase().split(" ").join("_"))] = answer
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
                    ENROLMENTSTATUS: item && item.userCareProgramPlan && item.userCareProgramPlan.state || "-",
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
            XLSX.writeFile(workbook, path.resolve(__dirname, dirName, `onboarding-user-enrolment-list-xlsx-${new Date().getTime()}.xlsx`))
            fs.writeFileSync(path.resolve(__dirname, dirName, `onboarding-user-enrolment-list-json-${new Date().getTime()}.json`), JSON.stringify(patients, null, 2));
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