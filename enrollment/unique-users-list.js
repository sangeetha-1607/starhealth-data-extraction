var MongoClient = require('mongodb').MongoClient;
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
            const usersModel  = database.collection("users");
            
            const userCareprogramsplans = await usersModel.find().toArray()
       
            const patients = userCareprogramsplans.map((item)=>{
                const user = item.userData
                const dobDate = user.dob && new Date(user.dob);
                const dob = dobDate && dobDate.getDate()+"-"+(dobDate.getMonth()+1)+"-"+dobDate.getFullYear();
                let userObject = {
                    firstName: user && user.name.first || "-",
                    lastName: user && user.name.last || "-",
                    mobile: user && user.mobile || "-",
                    email: user && user.email || "-",
                    dob: user && dob || "-",
                    
                };
                return userObject
            })
            
            fs.writeFileSync(`unique-user-list-${new Date().getTime()}.json`, JSON.stringify(patients, null, 2));
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