var MongoClient = require('mongodb').MongoClient;
const fs = require('fs');

async function main(){
    const uri = "mongodb://appAdmin:HLASr!E*66Xm@wellness-cmp-db.cluster-c1sodybcij48.ap-south-1.docdb.amazonaws.com:27017/cmp-prod?tls=false&authSource=admin&retryWrites=false";
    // const client = new MongoClient(uri, {  useNewUrlParser: true, useUnifiedTopology: true } );
    try {
            console.log("Connecting to DB")


            const client = new MongoClient(uri, {
              tlsCAFile: `./rds-combined-ca-bundle.pem`,
              useUnifiedTopology: true 
              })
            await client.connect();
            console.log("MongoDB connected!!!")
            const database = client.db("cmp-prod");
            const careProgrammeModel  = database.collection("care-programmes");
            const onboardingQuestionsModel  = database.collection("onboarding-questions");
            
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
            ];
            const userCareprogramsplans = await careProgrammeModel.aggregate(cpAgg).toArray()
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
                }
              ];
            const onboardingQuestions = await onboardingQuestionsModel.aggregate(obqAgg).toArray()
            const cpqAgg = [
                {
                  $match: {
                    name: { $regex: "^Condition Management Program$" },
                  },
                },
                {
                  $lookup: {
                    from: "care-programme-questions",
                    localField: "_id",
                    foreignField: "careProgramme",
                    as: "careProgrammeQuestions",
                  },
                },
                {
                  $unwind: {
                    path: "$careProgrammeQuestions",
                    preserveNullAndEmptyArrays: false,
                  },
                },
                {
                    $lookup: {
                      from: "questions",
                      localField: "careProgrammeQuestions.question",
                      foreignField: "_id",
                      as: "careProgrammeQuestions.question",
                    },
                },
                {
                    $unwind: {
                        path: "$careProgrammeQuestions.question",
                        preserveNullAndEmptyArrays: false,
                    },
                }
              ];
            const goalQuestions = await careProgrammeModel.aggregate(cpqAgg).toArray();
            let goalQuestionsNameMap = {}
            let questionNamesMap = {}
            
            const onboardingQuestionMap = onboardingQuestions.reduce((acc, item)=>{
                acc[String(item._id)] = JSON.parse(JSON.stringify(item));
                questionNamesMap[String(item.question.title.toLowerCase().split(" ").join("_"))]={}
                return acc; 
            },{});
            const goalQuestionsMap = goalQuestions.reduce((acc, item)=>{
                acc[String(item.careProgrammeQuestions._id)] = JSON.parse(JSON.stringify(item.careProgrammeQuestions));
                goalQuestionsNameMap[String(item.careProgrammeQuestions.question.title.toLowerCase().split(" ").join("_"))]={}
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
                let onboardingQues = Object.assign({}, questionNamesMap );
                user.onboardingQuestions.forEach(item=>{
                    let answer = item.answer;
                    if(onboardingQuestionMap[item.onboardingQuestion].question["type"] === "multiple-choice-multi-select"){
                        answer =onboardingQuestionMap[item.onboardingQuestion].question.options.filter(opItem=>item.answer.indexOf(String(opItem._id))>0).map(i=>i.value).join(", ")
                    }
                    if(onboardingQuestionMap[item.onboardingQuestion].question["type"] === "multiple-choice-single-select"){
                        answer =onboardingQuestionMap[item.onboardingQuestion].question.options.find(opItem=>String(item.answer) === String(opItem._id)).value
                    }
                    onboardingQues[String(onboardingQuestionMap[item.onboardingQuestion].question.title.toLowerCase().split(" ").join("_"))] = Object.assign({}, onboardingQuestionMap[item.onboardingQuestion], {answer})
                })

                let screeningQues = Object.assign({}, goalQuestionsNameMap );
                item.screeningQuestions && item.screeningQuestions.forEach(sqitem=>{
                    let answer = sqitem.answer;
                    if(goalQuestionsMap[sqitem.careProgrammeQuestion].question["type"] === "multiple-choice-multi-select"){
                        answer =goalQuestionsMap[sqitem.careProgrammeQuestion].question.options.filter(opItem=>sqitem.answer.indexOf(String(opItem._id))>0).map(i=>i.value).join(", ")
                    }
                    if(goalQuestionsMap[sqitem.careProgrammeQuestion].question["type"] === "multiple-choice-single-select"){
                        answer =goalQuestionsMap[sqitem.careProgrammeQuestion].question.options.find(opItem=>String(sqitem.answer) === String(opItem._id)).value
                    }
                    screeningQues[String(goalQuestionsMap[sqitem.careProgrammeQuestion].question.title.toLowerCase().split(" ").join("_"))] = Object.assign({}, goalQuestionsMap[sqitem.careProgrammeQuestion], {answer})
                })
                
                let userObject = {
                    firstName: user.name.first || "-",
                    lastName: user.name.last || "-",
                    mobile: user.mobile || "-",
                    email: user.email || "-",
                    gender: user.gender || "-",
                    dob: user.dob || "-",
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
                    city: address.city || "-",
                    state: address.state || "-",
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
                Object.assign(userObject, onboardingQues, screeningQues)
                return userObject
            })

            fs.writeFileSync('users-cohort-2.json', JSON.stringify(patients, null, 2));
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