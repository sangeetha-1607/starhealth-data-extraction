var MongoClient = require('mongodb').MongoClient;
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
            const questionsModel  = database.collection("questions");
            // const onboardingQuestionsModel  = database.collection("onboarding-questions");
            
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
              },
              {
                $match: {
                    "careProgrammePlan.userCareProgramPlan.state": "active"
                }
              }
            ];
            const userCareprogramsplans = await careProgrammeModel.aggregate(cpAgg).toArray()
            let physicalActivityQuestions = [
              new RegExp("How often could you able to overcome stress?", "i"),
              new RegExp("How often do you feel low/depressed?", "i"),
              new RegExp("How often do you get irritated?", "i"),
              new RegExp("How focused are you?", "i"),
              new RegExp("Do you follow any relaxation techniques (like meditation, breathing exercises etc)?", "i"),
              new RegExp("Could you able to manage your emotions?", "i")
            ];

            
            const cpqAgg = [
              {
                $match: {
                  title: { $in: physicalActivityQuestions },
                },
              },
              {
                $lookup: {
                  from: "care-programme-questions",
                  localField: "_id",
                  foreignField: "question",
                  as: "careProgrammeQuestions",
                },
              },
              {
                $unwind: {
                  path: "$careProgrammeQuestions",
                  preserveNullAndEmptyArrays: false,
                },
              }
            ];

            const goalQuestions = await questionsModel.aggregate(cpqAgg).toArray();
            let goalQuestionsNameMap = {}

            const goalQuestionsMap = goalQuestions.reduce((acc, item)=>{
                acc[String(item.careProgrammeQuestions._id)] = JSON.parse(JSON.stringify(item));
                goalQuestionsNameMap[String(item.title.toLowerCase().split(" ").join("_"))]={}
                return acc; 
            },{});
            const patients = userCareprogramsplans.map((item)=>{
                const user = item.careProgrammePlan && item.careProgrammePlan.userCareProgramPlan && item.careProgrammePlan.userCareProgramPlan.user
                const currDate = new Date()
                const dobDate = user.dob && new Date(user.dob);
                const age = dobDate && currDate.getFullYear()-dobDate.getFullYear();
                const [address] = user.addresses
                const height = user.medicalProfile && user.medicalProfile.height;
                const weight = user.medicalProfile && user.medicalProfile.weight ? user.medicalProfile.weight : user.medicalProfile && user.medicalProfile.recentVitals && user.medicalProfile.recentVitals.vital_body_weight;
                const bmi = (height && weight) && Number.parseFloat(Number.parseFloat((weight/(height*height))*10000).toFixed(2));

                let screeningQues = Object.assign({}, goalQuestionsNameMap );
                // console.log("item.careProgrammePlan.userCareProgramPlan.screeningQuestions", item.careProgrammePlan.userCareProgramPlan.screeningQuestions && JSON.stringify(item.careProgrammePlan.userCareProgramPlan.screeningQuestions[0], null, 2))
                let updatedScreeningQues = item.careProgrammePlan.userCareProgramPlan.screeningQuestions && item.careProgrammePlan.userCareProgramPlan.screeningQuestions.reduce(async (acc, sqitem)=>{
                    let answer = sqitem.answer;
                    if(goalQuestionsMap[sqitem.careProgrammeQuestion]){
                      if(goalQuestionsMap[sqitem.careProgrammeQuestion]["type"] === "multiple-choice-multi-select"){
                        answer =goalQuestionsMap[sqitem.careProgrammeQuestion].options.filter(opItem=>sqitem.answer.indexOf(String(opItem._id))>-1).map(i=>i.title).join(", ")
                      }
                      if(goalQuestionsMap[sqitem.careProgrammeQuestion]["type"] === "multiple-choice-single-select"){
                          answer =goalQuestionsMap[sqitem.careProgrammeQuestion].options.find(opItem=>String(sqitem.answer) === String(opItem._id)).title
                          console.log("sqitem.answer", sqitem.answer)
                          console.log("answer", answer)
                      }
                      acc[String(goalQuestionsMap[sqitem.careProgrammeQuestion].title.toLowerCase().split(" ").join("_"))] = Object.assign({}, goalQuestionsMap[sqitem.careProgrammeQuestion], {answer})
                      // console.log("Object.assign({}, goalQuestionsMap[sqitem.careProgrammeQuestion], {answer})", Object.assign({}, goalQuestionsMap[sqitem.careProgrammeQuestion], {answer}))
                      return await Promise.resolve(acc)
                    }
                }, screeningQues)
                console.log("updatedScreeningQues", JSON.stringify(updatedScreeningQues, null, 2))
                let userObject = {
                    firstName: user && user.name.first || "-",
                    lastName: user && user.name.last || "-",
                    mobile: user && user.mobile || "-",
                    email: user && user.email || "-",
                    gender: user && user.gender || "-",
                    dob: user && user.dob || "-",
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
                };
                Object.assign(userObject,  updatedScreeningQues)
                return userObject
            })
            
            // fs.writeFileSync(`cohort-2-screening-physical-activity-${new Date().getTime()}.json`, JSON.stringify(patients, null, 2));
            // process.exit(0);

            const workbook = XLSX.utils.book_new();
            var worksheet = XLSX.utils.json_to_sheet(patients, {
              header: Object.keys(patients[0]),
            });
            XLSX.utils.book_append_sheet(workbook, worksheet);
   
            let currTime = new Date()
            // let dirName = currTime.toISOString().split("T").join("-").split(":").join("-").split(".")[0]
            // fs.mkdirSync(path.join(__dirname, dirName));
            // console.log('Directory created successfully!', dirName);
            // XLSX.writeFile(workbook, path.resolve(__dirname, dirName, `cohort-2-screening-mindfullness-xlsx-${new Date().getTime()}.xlsx`))
            // fs.writeFileSync(path.resolve(__dirname, dirName, `cohort-2-screening-mindfullness-json-${new Date().getTime()}.json`), JSON.stringify(patients, null, 2));
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