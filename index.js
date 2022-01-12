const fs = require("fs");
const readline = require("readline");
const AWS = require("aws-sdk");
const uuid = require("uuid");

const fileName = "archivo.csv";
const batchSize = 25;
const concurrentRequests = 40;

const dynamodb = new AWS.DynamoDB.DocumentClient();

function convertToObject(line) {
  const items = line.split(",");
  return {
    id: uuid.v4(),
    name: items[0],
    lastName: items[1],
  };
}

async function saveToDynamoDB(items, table = "test_table") {
  const putReqs = items.map((item) => ({
    PutRequest: {
      Item: item,
    },
  }));

  const params = {
    RequestItems: {
      [table]: putReqs,
    },
  };

  await dynamodb.batchWrite(params).promise();
}

(async function () {
  const startTime = Date.now();
  const readStream = fs.createReadStream(fileName, { encoding: "utf8" });
  const rl = readline.createInterface({
    input: readStream,
    crlfDelay: Infinity,
  });

  let firstLine = true;
  let items = [];
  let batchAmount = 0;
  let promises = [];

  for await (const line of rl) {
    if (firstLine) {
      firstLine = false;
      continue;
    }

    const obj = convertToObject(line);
    if (obj) {
      items.push(obj);
    }

    if (items.length % batchSize === 0) {
      console.log(` batch ${batchAmount}`);

      promises.push(saveToDynamoDB(items));

      if (promises.length % concurrentRequests === 0) {
        console.log("\nAwaiting write requests to DynamoDB\n");
        await Promise.all(promises);
        promises = [];
      }

      items = [];
      batchAmount++;
    }
  }

  if (items.length > 0) {
    console.log(` batch ${batchAmount}`);
    promises.push(saveToDynamoDB(items));
  }

  if (promises.length > 0) {
    console.log("\nAwaiting write to DynamoDB\n");
    await Promise.all(promises);
  }
  const takenSecs = (Date.now() - startTime) / 1000;
  console.log(`Job done in ${takenSecs} seconds`);
})();
