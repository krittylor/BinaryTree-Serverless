'use strict';
/**
 * Created by admin on 6/9/17.
 */
let AWS = require('aws-sdk');
let Promise = require('promise');
let wrapper = require('co-express');
let attr = require('dynamodb-data-types').AttributeValue;
let docClient = new AWS.DynamoDB.DocumentClient();

//Read table8 references
function readLimits(){
  const params = {
    TableName: 'table8_reference'
  };
  return docClient.scan(params).promise().then(res => res.Items);
}
let adsIncome = 0;
let limits = [];
let scanNumbers = [];
let totalScans = 0;
let limitObjects;
let adsIncomes = {};
function getRecordsFromTable6() {
  let params = {
    TableName: 'table6_relatedrecords',
    FilterExpression: 'fullHouse=:value',
    ExpressionAttributeValues: {
      ':value': false
    }
  };
  return docClient.scan(params).promise().then(data => data.Items ? data.Items : []);
}

function getRecordsFromTable1(uniqueid){
  let params = {
    TableName: 'table1_geoqrcoderecord',
    KeyConditionExpression: 'uniqueid=:value',
    ExpressionAttributeValues: {
      ':value': uniqueid
    },
    ProjectionExpression: 'uniqueid, fixedornot'
  };
  return docClient.query(params).promise().then(data => data.Items ? data.Items : []);
}

function getAdsIncomes(){
  let params = {
    TableName: 'table20_ads_income'
  };
  return docClient.scan(params).promise()
    .then(data => {
      if(data.Items){
        return adsIncome = data.Items[data.Items.length - 1].ads_income;
      }
      return 0;
    })
    .catch(err => {
      console.log(err);
    })
}
function getLimit(leftCount, rightCount) {
  const min = Math.min(leftCount, rightCount);
  for (let i = limits.length - 1; i >= 0; i--) {
    if (min >= limits[i]) {
      return limits[i];
    }
  }
  return limits[0];
}

function getReferenceFromLimit(limit) {
  for (let i = 0; i < limits.length; i++){
    if(limitObjects[i].relatedbelow01number == limit)
      return limitObjects[i].reference;
  }
  return 0;
}
function getFixedOrNotCount(records, value){
  let count = 0;
  records.forEach(record => {
    if(record.fixedornot === value)
      count++;
  });
  return count;
}

function getRecordFromTable3(uniqueid){
  let params = {
    TableName: 'table3_advanceuserinfo_spendingrecord',
    KeyConditionExpression: 'uniqueid=:value',
    ExpressionAttributeValues: {
      ':value': uniqueid
    }
  };
  return docClient.query(params).promise()
    .then(data => data.Items[0] ? data.Items[0] : undefined);
}

function getPastReports(uniqueid){
  let params = {
    TableName: 'table9_report',
    KeyConditionExpression: 'uniqueid=:value',
    ExpressionAttributeValues: {
      ':value': uniqueid
    }
  };
  return docClient.query(params).promise()
    .then(data => data.Items ? data.Items : []);
}

function getRelatedBelowCount(uniqueid) {
  let params = {
    TableName: "table6_relatedrecords",
    IndexName: "RELATEDABOVE",
    KeyConditionExpression: "#attr = :value",
    ExpressionAttributeNames: {
      "#attr": "relatedabove"
    },
    ExpressionAttributeValues: {
      ":value": uniqueid
    }
  };
  return docClient.query(params).promise().then(data => data.Items[0] ? data.Items.length : 0);
}

function getScanNumbers(){
  let params = {
    TableName: "table12_sync_table1"
  };
  return docClient.scan(params).promise().then(data => data.Items);
}

function getScanNumberFromUniqueId(uniqueid){
  if(scanNumbers){
    for (let i = 0; i < scanNumbers.length; i++)
      if(scanNumbers[i].uniqueid == uniqueid)
        return scanNumbers[i];
  }
  return {
    uniqueid,
    fixedscannumber: 0,
    mobilescannumber: 0
  }
}

let handler = function(event, context) {
  let rowsCountOfTable1 = 1;
  getAdsIncomes()
    .then(() => {
      console.log('GetScanNumbers');
      return getScanNumbers();
    })
    .then(scans => {
      scanNumbers = scans;
      totalScans = 0;
      scanNumbers.forEach(scan => {
        totalScans += scan.fixedscannumber + scan.mobilescannumber;
      });
      console.log('ReadLimits');
      return readLimits();
    })
    .then(res => {
      limitObjects = res;
      console.log(res);
      res.sort((a, b) => {
        if (a.point > b.point)
          return 1;
        if (a.point < b.point)
          return -1;
        return 0;
      });
      limits = [];
      res.forEach(function (a) {
        limits.push(a.relatedbelow01number);
      });
      console.log("Limits: ", limits);
      return getRecordsFromTable6();
    })
    .then(records => {
      console.log(records);
      records.forEach(wrapper(function*(record){
        try {
          //Get fixedornot=true count
          const uniqueid = record.uniqueid;
          const scan = getScanNumberFromUniqueId(uniqueid);
          let fixedscannumber = scan.fixedscannumber;
          let mobilescannumber = scan.mobilescannumber;
          let recordFromTable3 = yield getRecordFromTable3(uniqueid);
          let pastReports = yield getPastReports(uniqueid);
          let pastAccumulatedTotal = pastReports.length > 0 ? pastReports[pastReports.length - 1].accumulatedtotal : 0;
          const paymentstatus = recordFromTable3.paymentstatus;
          const ads_income = adsIncome || 0;
          const totalscanon28 = totalScans;

          const scanratio = (scan.fixedscannumber + scan.mobilescannumber) / totalscanon28;

          const amountfortotalscan = scanratio * ads_income;
          let amountforadvanceuser = 0;
          const relatedLeftCount = record.relatedLeftCount;
          const relatedRightCount = record.relatedRightCount;
          //Get from table6, 8
          let qualifier = false;
          let paymentqualifer = false;
          let monthgrandtotal = 0;
          const relatedBelowCount = yield getRelatedBelowCount(uniqueid);
          if (paymentstatus && fixedscannumber >= 4) {
            qualifier = true;
            if (relatedBelowCount >= 2){
              paymentqualifer = true;
              const recordLimit = getLimit(relatedLeftCount, relatedRightCount);
              amountforadvanceuser = getReferenceFromLimit(recordLimit);
              monthgrandtotal = amountfortotalscan + amountforadvanceuser;
            } else {
              paymentqualifer = false;
              monthgrandtotal = amountfortotalscan;
            }
          }
          if (!paymentstatus && fixedscannumber >= 30) {
            qualifier = true;
            // const recordLimit = getLimit(relatedLeftCount, relatedRightCount);
            // amountforadvanceuser = getReferenceFromLimit(recordLimit);
            monthgrandtotal = amountfortotalscan;
          }
          const date = new Date();
          const year = date.getUTCFullYear();
          const month = date.getUTCMonth() + 1;
          const day = date.getUTCDate();
          const hours = date.getUTCHours();
          const reportdatehour = `${year}${month > 9 ? month : '0' + month}` +
            `${day > 9 ? day : '0' + day}` +
            `${hours > 9 ? hours : '0' + hours}`;

          const accumulatedtotal = pastAccumulatedTotal + monthgrandtotal;
          let params = {
            TableName: 'table9_report',
            Item: {
              uniqueid,
              accumulatedtotal,
              ads_income,
              paymentstatus,
              amountforadvanceuser,
              amountfortotalscan,
              fixedscannumber,
              mobilescannumber,
              monthgrandtotal,
              qualifier,
              reportdatehour,
              scanratio,
              totalscanon28,
              paymentqualifer
            }
          };
          docClient.put(params).promise()
            .then(data => {
              console.log('Generated report for ', params.Item);
            })
            .catch(err => {
              console.log(err);
            });
        } catch(err) {
          console.trace(err);
        }
      }))
    })
    .catch(err => {
      console.trace(err);
    });
};
module.exports.handler = handler;

if(require.main === module) {
  handler({}, {
    succeed: function (res) {
      console.log("Succeeded");
      console.log(res);
    }
  });
}