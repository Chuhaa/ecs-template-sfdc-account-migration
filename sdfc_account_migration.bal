import ballerina/io;
import ballerina/config;
import ballerinax/mysql;
import ballerina/'log;
import ballerina/sql;
import ballerinax/sfdc;

sfdc:SalesforceConfiguration sfConfig = {
    baseUrl: config:getAsString("SF_EP_URL"),
    clientConfig: {
        accessToken: config:getAsString("SF_ACCESS_TOKEN"),
        refreshConfig: {
            clientId: config:getAsString("SF_CLIENT_ID"),
            clientSecret: config:getAsString("SF_CLIENT_SECRET"),
            refreshToken: config:getAsString("SF_REFRESH_TOKEN"),
            refreshUrl: config:getAsString("SF_REFRESH_URL")
        }
    }
};

sfdc:ListenerConfiguration listenerConfig = {
    username: config:getAsString("SF_USERNAME"),
    password: config:getAsString("SF_PASSWORD")
};

sfdc:BaseClient baseClient = new(sfConfig);

listener sfdc:Listener sfdcEventListener = new (listenerConfig);
mysql:Client mysqlClient =  check new (user = config:getAsString("DB_USER"),
                                        password = config:getAsString("DB_PWD"));

public function main(){
    string queryStr = "SELECT Id FROM Account";
    error|sfdc:BulkJob queryJob = baseClient->creatJob("query", "Account", "JSON");
    if (queryJob is sfdc:BulkJob) {
        error|sfdc:BatchInfo batch = queryJob->addBatch(queryStr);
        if (batch is sfdc:BatchInfo) {
            string batchId = batch.id;
            var batchResult = queryJob->getBatchResult(batchId);
            if (batchResult is json) {
                json[]|error batchResultArr = <json[]>batchResult;
                if (batchResultArr is json[]) {
                    foreach var result in batchResultArr {
                        string accountId = result.Id.toString();
                        log:print("Account ID : " + accountId); 
                        migrateAccount(accountId);
                    }
                } else {
                    log:printError(batchResultArr.toString());
                }
            } else if (batchResult is error) {
                log:printError(batchResult.message());
            } else {
                log:printError("Invalid Batch Result!");
            }    
        } else {
            log:printError(batch.message());
        }
    }
    else{
        log:printError(queryJob.message());
    }
}

@sfdc:ServiceConfig {
    topic:config:getAsString("SF_ACCOUNT_TOPIC")
}
service on sfdcEventListener {
    remote function onEvent(json acc) {  
        io:StringReader sr = new(acc.toJsonString());
        json|error account = sr.readJson();
        if (account is json) {
            log:print(account.toJsonString());
            string accountId = account.sobject.Id.toString();
            log:print("Account ID : " + accountId);
            migrateAccount(accountId);
        }
    }
}

function migrateAccount(string accountId) {
    json|sfdc:Error accountInfo = baseClient->getAccountById(accountId);
    if (accountInfo is json) {
        addAccountToDB(<@untainted>accountInfo);
    }
}

function addAccountToDB(json account) {
    string id = account.Id.toString();
    string name = account.Name.toString();
    string accType = account.Type.toString();
    string phone = account.Phone.toString();
    string fax = account.Fax.toString();
    string accountNumber = account.AccountNumber.toString();
    string website = account.Website.toString();
    string industry = account.Industry.toString();
    string accOwnership = account.Ownership.toString();
    string description = account.Description.toString();
    
    log:print(id + ":" + name + ":" + accType + ":" + description );
    sql:ParameterizedQuery insertQuery =
            `INSERT INTO ESC_SFDC_TO_DB.Account (AccountId, Name, Type, Phone, Fax, AccountNumber, Website, Industry, Ownership, Description) 
            VALUES (${id}, ${name}, ${accType}, ${phone}, ${fax}, ${accountNumber}, ${website}, ${industry}, ${accOwnership}, ${description})
            ON DUPLICATE KEY UPDATE AccountId = ${id}, Name = ${name}, Type = ${accType}, Phone = ${phone}, Fax = ${fax}, AccountNumber = ${accountNumber},
            Website =${website}, Industry = ${industry}, Ownership = ${accOwnership}, Description = ${description}`;
    sql:ExecutionResult|sql:Error? result  = mysqlClient->execute(insertQuery);
}
