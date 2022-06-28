var MongoClient = require('mongodb').MongoClient;
const {ObjectId} = require('mongodb');
const fs = require('fs');

async function main(){
    // const uri = "mongodb://appAdmin:HLASr!E*66Xm@wellness-cmp-db.cluster-c1sodybcij48.ap-south-1.docdb.amazonaws.com:27017/cmp-prod?tls=false&authSource=admin&retryWrites=false";
    const uri = "mongodb://localhost:27018";
    // const client = new MongoClient(uri, {  useNewUrlParser: true, useUnifiedTopology: true } );
    let hraUsersRawdata = fs.readFileSync('./hra/hra-users-1656330768537.json');
    let hraUserData = JSON.parse(hraUsersRawdata);
    // console.log("hraUserData", hraUserData[0])
    try {
            console.log("Connecting to DB")
            const client = new MongoClient(uri, {
              // tlsCAFile: `./rds-combined-ca-bundle.pem`,
              // useUnifiedTopology: true 
              })
            await client.connect();
            console.log("MongoDB connected!!!")
            // const database = client.db("cmp-prod");
            const database = client.db("rpm-local");
            const usersModel  = database.collection("users");
            const userDataParametersModel  = database.collection("user-data-parameters");
            const parametersModel  = database.collection("parameters");
            const onboardingQuestionsModel  = database.collection("onboarding-questions");
            // const onboardingQuestionsModel  = database.collection("onboarding-questions");
            const obq = ["What is your waist circumference (inches)?"]
            const users = await usersModel.find({mobile: {$in:["9884562500", "7063243221"]}}).toArray();
            const parameters = await parametersModel.find({code:{$in:["21112-8", "62791-9", "99285-9", "8280-0", "LL2191-6"]}}).project({}).toArray();
 
            const hraUserDataMap = hraUserData.reduce(
              (acc, item) => Object.assign(acc, {[item.Mobile]: item}),
              {}
            );
            const parametersMap = parameters.reduce(
              (acc, item) => Object.assign(acc, {[item.code]: item}),
              {}
            );
            console.log("parametersMap", Object.keys(parametersMap))
            let userDataParameterToInsert =  users.reduce((acc, item)=>{
              let DOBUserDataParameter={
                user: ObjectId(item._id),
                dataParameter: ObjectId(parametersMap["21112-8"]._id),
                value: new Date(item?.dob),
                createdAt: new Date(),
                updatedAt: new Date()
              };
              let genderUserDataParameter={
                user: ObjectId(item._id),
                dataParameter: ObjectId(parametersMap["LL2191-6"]._id),
                createdAt: new Date(),
                updatedAt: new Date()
              };
              let waistCircumferenceUserDataParameter={
                user: ObjectId(item._id),
                dataParameter: ObjectId(parametersMap["8280-0"]._id),
                value: hraUserDataMap[item.mobile]["Waist Circumference (cm)"],
                createdAt: new Date(),
                updatedAt: new Date()
              };
              let physicalActivityUserDataParameter={
                user: ObjectId(item._id),
                dataParameter: ObjectId(parametersMap["99285-9"]._id),
                createdAt: new Date(),
                updatedAt: new Date()
              };
              let familyHistoryUserDataParameter={
                user: ObjectId(item._id),
                dataParameter: ObjectId(parametersMap["62791-9"]._id),
                createdAt: new Date(),
                updatedAt: new Date()
              };
              if(item.gender === "male"){
                genderUserDataParameter.value = "LA2-8";
              }
              else if(item.gender === "female"){
                genderUserDataParameter.value = "LA3-6";
              }
              
              if(hraUserDataMap[item.mobile]["Describe your activity routine"].trim() === "Moderately Active (moderate exercise 3-5 days / week)"){
                physicalActivityUserDataParameter.value = "LA16666-2";
              }
              else if(hraUserDataMap[item.mobile]["Describe your activity routine"].trim() === "Lightly Active (light exercise 1-3 days / week)"){
                physicalActivityUserDataParameter.value = "LA32776-9";
              }
              else if(hraUserDataMap[item.mobile]["Describe your activity routine"].trim() === "Sedentary (little to no exercise + work a desk job)"){
                physicalActivityUserDataParameter.value = "LA32775-1";
              }
              else if(hraUserDataMap[item.mobile]["Describe your activity routine"].trim() === "Extremely Active (strenuous training 2x / day)"){
                physicalActivityUserDataParameter.value = "SC31";
              }
              else if(hraUserDataMap[item.mobile]["Describe your activity routine"].trim() === "Very Active (heavy exercise 6-7 days / week)"){
                physicalActivityUserDataParameter.value = "LA32778-5";
              }
              if(hraUserDataMap[item.mobile]["Do you have a family history of diabetes?"].trim() === "One parent has Diabetes"){
                familyHistoryUserDataParameter.value = "SC28";
              }
              else if(hraUserDataMap[item.mobile]["Do you have a family history of diabetes?"].trim() === "Both parents do not have Diabetes"){
                familyHistoryUserDataParameter.value = "SC30";
              }
              else if(hraUserDataMap[item.mobile]["Do you have a family history of diabetes?"].trim() === "Both parents have Diabetes"){
                familyHistoryUserDataParameter.value = "SC29";
              }
              return [...acc, waistCircumferenceUserDataParameter, physicalActivityUserDataParameter, familyHistoryUserDataParameter, genderUserDataParameter, DOBUserDataParameter]
            }, [])
            await userDataParametersModel.insertMany(userDataParameterToInsert)

            
    
    } catch (e) {
      console.log(e)
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