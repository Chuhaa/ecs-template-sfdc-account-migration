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



@sfdc:ServiceConfig {
    topic:config:getAsString("SF_ACCOUNT_TOPIC")
}

service sfdcAccountListener on sfdcEventListener {
    resource function onEvent(json acc) {  
        //convert json string to json
        io:StringReader sr = new(acc.toJsonString());
        json|error account = sr.readJson();
        if (account is json) {
            log:printInfo(account.toJsonString());
            //Get the account id from the account
            string accountId = account.sobject.Id.toString();
            log:printInfo("Account ID : " + accountId);
            json|sfdc:Error accountInfo = baseClient->getAccountById(accountId);
            if (accountInfo is json) {
                // Log account information. 
                log:printInfo(accountInfo);
                // Add the current account to a DB. 
                sql:Error? result  = addAccountToDB(<@untainted>accountInfo);
                if (result is error) {
                    log:printError(result.message());
                }
            }

        }
    }
}


function addAccountToDB(json account) returns sql:Error? {
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
    
    log:printInfo(id + ":" + name + ":" + accType + ":" + description );
    // The SQL query to insert an account record to the DB. 
    sql:ParameterizedQuery insertQuery =
            `INSERT INTO ESC_SFDC_TO_DB.Account (AccountId, Name, Type, Phone, Fax, AccountNumber, Website, Industry, Ownership, Description) 
            VALUES (${id}, ${name}, ${accType}, ${phone}, ${fax}, ${accountNumber}, ${website}, ${industry}, ${accOwnership}, ${description})`;
    // Invoking the MySQL Client to execute the insert operation. 
    sql:ExecutionResult result  =  check mysqlClient->execute(insertQuery);
}
