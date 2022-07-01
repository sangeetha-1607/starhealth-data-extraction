var MongoClient = require('mongodb').MongoClient;
const {ObjectId} = require('mongodb');
const fs = require('fs');

async function main(){
    // const uri = "mongodb://appAdmin:HLASr!E*66Xm@wellness-cmp-db.cluster-c1sodybcij48.ap-south-1.docdb.amazonaws.com:27017/cmp-prod?tls=false&authSource=admin&retryWrites=false";
    const uri = "mongodb://starhealth:STarHEalTH09346@docdb-2021-09-16-07-12-37.cluster-cmmxdqgb2co0.ap-south-1.docdb.amazonaws.com:27017/star-health-dev?authSource=admin&readPreference=primary&appname=MongoDB%20Compass&ssl=true&retryWrites=false";
    // const uri = "mongodb://localhost:27018";
    // const client = new MongoClient(uri, {  useNewUrlParser: true, useUnifiedTopology: true } );
    let hraUsersRawdata = fs.readFileSync('./hra/hra-users-1656330768537.json');
    let hraUserData = JSON.parse(hraUsersRawdata);
    // console.log("hraUserData", hraUserData[0])
    try {
            console.log("Connecting to DB")
            const client = new MongoClient(uri, {
              tlsCAFile: `./rds-combined-ca-bundle.pem`,
              useUnifiedTopology: true 
              })
            await client.connect();
            console.log("MongoDB connected!!!")
            // const database = client.db("cmp-prod");
            const database = client.db("star-health-dev");
            // const database = client.db("rpm-local")
            const usersModel  = database.collection("users");
            const userDataParametersModel  = database.collection("user-data-parameters");
            const parametersModel  = database.collection("parameters");
            const users = await usersModel.find({mobile: {$in:["9884562500", "7063243221", "9790635291", "9790900828"]}}).toArray();
            const parameters = await parametersModel.find({code:{$in:["21112-8", "62791-9", "99285-9", "8280-0", "LL2191-6", "72166-2", "38341003"]}}).toArray();
 
            const hraUserDataMap = hraUserData.reduce(
              (acc, item) => Object.assign(acc, {[item.Mobile]: item}),
              {}
            );
            const parametersMap = parameters.reduce(
              (acc, item) => Object.assign(acc, {[item.code]: item}),
              {}
            );
            let userDataParameterToInsert =  users.reduce((acc, item)=>{
              console.log("item", item.name, item.mobile)
              if(hraUserDataMap[item.mobile]){
                let DOBUserDataParameter={
                  user: ObjectId(item._id),
                  dataParameter: ObjectId(parametersMap["21112-8"]._id),
                  value: new Date(item?.dob),
                  isScoreCalculated: false,
                  createdAt: new Date(),
                  updatedAt: new Date()
                };
                let genderUserDataParameter={
                  user: ObjectId(item._id),
                  dataParameter: ObjectId(parametersMap["LL2191-6"]._id),
                  createdAt: new Date(),
                  updatedAt: new Date(),
                  isScoreCalculated: false
                };
                let waistCircumferenceUserDataParameter={
                  user: ObjectId(item._id),
                  dataParameter: ObjectId(parametersMap["8280-0"]._id),
                  value: hraUserDataMap[item.mobile]["Waist Circumference (cm)"],
                  createdAt: new Date(),
                  updatedAt: new Date(),
                  isScoreCalculated: false
                };
                let physicalActivityUserDataParameter={
                  user: ObjectId(item._id),
                  dataParameter: ObjectId(parametersMap["99285-9"]._id),
                  createdAt: new Date(),
                  updatedAt: new Date(),
                  isScoreCalculated: false
                };
                let familyHistoryUserDataParameter={
                  user: ObjectId(item._id),
                  dataParameter: ObjectId(parametersMap["62791-9"]._id),
                  createdAt: new Date(),
                  updatedAt: new Date(),
                  isScoreCalculated: false
                };
                let currentlySmokingUserDataParameter={
                  user: ObjectId(item._id),
                  dataParameter: ObjectId(parametersMap["72166-2"]._id),
                  createdAt: new Date(),
                  updatedAt: new Date(),
                  isScoreCalculated: false
                };
                let hypertensionUserDataParameter={
                  user: ObjectId(item._id),
                  dataParameter: ObjectId(parametersMap["38341003"]._id),
                  createdAt: new Date(),
                  updatedAt: new Date(),
                  isScoreCalculated: false
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

                if(hraUserDataMap[item.mobile]["Do you smoke?"].trim() === "Yes"){
                  currentlySmokingUserDataParameter.value = "LA33-6";
                }
                else if(hraUserDataMap[item.mobile]["Do you smoke?"].trim() === "No"){
                  currentlySmokingUserDataParameter.value = "LA32-8";
                }

                if(hraUserDataMap[item.mobile]["Have you ever been diagnosed with Hypertension?"].trim() === "Yes" || hraUserDataMap[item.mobile]["Have you ever been diagnosed with Hypertension?"].trim()=== "Yes, but Not On Treatment" || hraUserDataMap[item.mobile]["Have you ever been diagnosed with Hypertension?"].trim()=== "Yes on Medicines"){
                  hypertensionUserDataParameter.value = "LA33-6";
                }
                else if(hraUserDataMap[item.mobile]["Have you ever been diagnosed with Hypertension?"].trim() === "No"){
                  hypertensionUserDataParameter.value = "LA32-8";
                }
                return [...acc, waistCircumferenceUserDataParameter, physicalActivityUserDataParameter, familyHistoryUserDataParameter, genderUserDataParameter, DOBUserDataParameter, currentlySmokingUserDataParameter, hypertensionUserDataParameter]
              }
              return acc;
              
            }, [])
            const udpinsert = await userDataParametersModel.insertMany(userDataParameterToInsert)
            console.log("Multiple insert result", udpinsert)
            
    
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