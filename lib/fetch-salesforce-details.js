const redis = require("redis");
const { promisify } = require("util");

require("dotenv").config();
const dev = process.env.NODE_ENV !== "production";

const salesforceCacheTTL = 86400; // 24-hours

// Setup Redis datastore to perform queries (separate from publisher)
// to get details that are missing from the CDC messages.
const REDIS_URL = process.env.REDIS_URL;
if (REDIS_URL == null) {
    throw new Error("Requires REDIS_URL env var.");
}
const redisQuery = redis.createClient(REDIS_URL);
const getAsync = promisify(redisQuery.get).bind(redisQuery);
const setAsync = promisify(redisQuery.set).bind(redisQuery);
redisQuery.on("error", function (err) {
    console.error(`redis query error: ${err.stack}`);
    process.exit(1);
});

const fetchSalesforceNameWithCache = (salesforceApi, sfid, type, payload) => {
    if (sfid == null) {
        throw new Error("Requires `sfid` parameter (arg 0)");
    }
    if (type == null) {
        throw new Error("Requires `type` parameter (arg 1)");
    }
    const cacheKey = `salesforce-cache:${sfid}`;
    const contextKey = `${type}Name`;

    console.log(`### fetchSalesforceNameWithCache : sfid = ${sfid}, type = ${type}`);

    return getAsync(cacheKey).then((cachedValue) => {
        if (cachedValue == null) {

            // Salesforce custom event
            if (payload) {
                return setAsync(
                    cacheKey,
                    payload.Payload__c,
                    "EX",
                    salesforceCacheTTL
                ).then(() => {
                    return { [contextKey]: payload.Payload__c };
                });
            } 
            
            // Salesforce ChangeEvent
            else {
                return salesforceApi
                    .sobject(type)
                    .select("Name")
                    .where(`Id = '${sfid}'`)
                    .execute()
                    .then((records) => {
                        if (records  &&  records[0] != null) {
                            return setAsync(
                                cacheKey,
                                records[0].Name,
                                "EX",
                                salesforceCacheTTL
                            ).then(() => {
                                return { [contextKey]: records[0].Name };
                            });
                        }
                    });
            }
        } else {
            return { [contextKey]: cachedValue };
        }
    });
};

const fetchSalesforceDetails = (message, salesforceApi) => {
    /*
    {
        "schema": "6s0jJJf1Znf29b8CBNbW_w",
        "payload": {
            "LastModifiedDate": "2020-04-11T21:49:45.000Z",
            "Name": "Test for stream11",
            "ChangeEventHeader": {
                "commitNumber": 381575860761,
                "commitUser": "0051i0000015ttbAAA",
                "sequenceNumber": 1,
                "entityName": "Account",
                "changeType": "UPDATE",
                "changedFields": [
                    "Name",
                    "LastModifiedDate"
                ],
                "changeOrigin": "",
                "transactionKey": "000053db-de82-8699-8b56-8f52bbdd5185",
                "commitTimestamp": 1586641785000,
                "recordIds": [
                    "0011i00000U359dAAB"
                ]
            }
        },
        "event": {
            "replayId": 3217203
        },
        "context": {
            "UserName": "Rupert Barrow",
            "AccountName": "Test for stream11"
        }
    }
    */

    /*
    {
        "schema": "0GGZ3m3XkrPworAjxmDUUQ",
        "payload": {
            "CreatedById": "0051i0000015ttbAAA",
            "Payload__c": "Détails de la commande",
            "Type__c": "Commande",
            "CreatedDate": "2020-04-16T14:58:40.567Z"
        },
        "event": {
            "replayId": 288436
        }
    }
    */

    const payload = message.payload || {};
    const event   = message.event || {};
    const header  = payload.ChangeEventHeader || {};
    const fetches = [];

    // Salesforce record
    if (header.commitUser != null) {
        fetches.push(fetchSalesforceNameWithCache(salesforceApi, header.commitUser,     'User',             null));
    }
    if (header.recordIds != null  &&  header.recordIds[0] != null) {
        fetches.push(fetchSalesforceNameWithCache(salesforceApi, header.recordIds[0],   header.entityName,  null));
    }
    
    // Salesforce custom event
    if (payload.CreatedById != null) {
        fetches.push(fetchSalesforceNameWithCache(salesforceApi, payload.CreatedById,   'User',             null));
    }
    if (payload.Type__c != null) {
        fetches.push(fetchSalesforceNameWithCache(salesforceApi, event.replayId,        payload.Type__c,    payload));
    }

    return Promise.all(fetches).then((results) => {
        message.context = {};
        // Merge the returned record details into the message "context" property.
        results.forEach(
            (r) => (message.context = { ...message.context, ...r })
        );
        return message;
    });
};

module.exports = fetchSalesforceDetails;
