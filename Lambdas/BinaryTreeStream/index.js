'use strict';
/**
 * Created by admin on 6/9/17.
 */
var AWS = require('aws-sdk');
var Promise = require('promise');
var wrapper = require('co-express');
var attr = require('dynamodb-data-types').AttributeValue;
// AWS.config.update({ accessKeyId: 'myid', secretAccessKey: 'mykey', region: 'us-east-1' });
// const dyn = new AWS.DynamoDB({endpoint: new AWS.Endpoint('http://localhost:8000')});
var docClient = new AWS.DynamoDB.DocumentClient();

let limits = [];

function pushNode(node){
  var params = {
    TableName: "table6_relatedrecords",
    Item : node
  };
  console.log('Putting Item : ');
  console.log(node);
  return docClient.put(params).promise().then(node => node);
}
function getNodeFromId(uniqueid){
  const params = {
    TableName: 'table6_relatedrecords',
    KeyConditionExpression: "uniqueid = :value",
    ExpressionAttributeValues: {
      ":value": uniqueid
    }
  };
  return docClient.query(params).promise()
      .then(data => {
        return data.Items[0] ? data.Items[0] : undefined});
}

function getLimit(leftCount, rightCount) {
  for (let i = 0; i < limits.length; i++) {
    if (leftCount <= limits[i] && rightCount <= limits[i]) {
      if (leftCount === rightCount && leftCount === limits[i]) {
        if (limits.length === i + 1)
          return limits[i];
        else
          return limits[i + 1];
      }
      return limits[i];
    }
  }
  return limits[0];
}

const PositionType = {
  LEFT: 'LEFT',
  RIGHT: 'RIGHT'
};

/**
 * Returns { positionType, directParent }
 * @param relatedabove - The related parent node that shows by who the node is introduced
 */
var getNewPosition = function(uniqueid){
  return new Promise((resolve, reject) => {
    console.log('uniqueid: ', uniqueid);
    getNodeFromId(uniqueid).then(node => {

      console.log(node);
      const leftCount = node.relatedLeftCount;
      const rightCount = node.relatedRightCount;

      // Get limit of current subtree
      const limit = getLimit(leftCount, rightCount);
      console.log('Current Limit is ', limit);
      //Exceeded limit of max
      /**
       * If limit is 1, return empty side of current node
       */
      console.log('leftCount: ', leftCount, ' rightCount: ', rightCount);
      if(limit == 1){
        console.log("limit is 1");
        if(leftCount == 0){
          console.log('Returning left and root');
          return resolve({ position: PositionType.LEFT, directAbove: uniqueid});
        }

        else if(rightCount == 0){
          console.log('Returning left and root');
          return resolve({ position: PositionType.RIGHT, directAbove: uniqueid});
        }
      }

      /**
       * There are 3 kinds of subtrees
       * 1. Left is full, right is free - will choose right
       * 2. Right is full, left is free - will choose left
       * 3. Both of them are free - will choose left
       */

      if(leftCount === limit && rightCount < limit){
        getNewPosition(node.relatedRight).then(res => resolve(res));
      }
      else if(rightCount === limit && leftCount < limit){
        getNewPosition(node.relatedLeft).then(res => resolve(res));
      }
      else {
        getNewPosition(node.relatedLeft).then(res => resolve(res));
      }
    });
  });
};

function getRoot(){
  var params = {
    TableName: "table6_relatedrecords",
    IndexName: "PATH",
    KeyConditionExpression: "#attr = :value",
    ExpressionAttributeNames: {
      "#attr": "path"
    },
    ExpressionAttributeValues: {
      ":value": "1"
    }
  };
  return docClient.query(params).promise().then(data => data.Items[0] ? data.Items[0] : undefined);
}
function addNode(node){
  return new Promise((resolve, reject) => {
    //Check left or right in tree
    //Only check if it's not root
    getRoot().then(wrapper(function*(root){
      // if(root){
      //   let parentNode = yield getNodeFromId(node.relatedabove);
      //   let path = parentNode.path;
      //   let leftCount = root.relatedLeftCount;
      //   let rightCount = root.relatedRightCount;
      //   const maxLimit = limits[limits.length - 1];
      //   //If parent is not root node, check availability.
      //   if (path.length >= 2) {
      //     if (path[1] === '0' && leftCount === maxLimit)
      //       throw new Error("Oops, left subtree is full!");
      //     if (path[1] === '1' && rightCount === maxLimit)
      //       throw new Error("Oops, right subtree is full!")
      //   }
      // }
      let newNode = {
        uniqueid: node.uniqueid,
        relatedabove: node.relatedabove,
        directAbove: undefined,
        path: "1",
        paymentdatehourminutesecond: node.paymentdatehourminutesecond,
        relatedLeft: undefined,
        relatedLeftCount: 0,
        relatedRight: undefined,
        relatedRightCount: 0,
        reEntryCompleted: false,
        fullHouse: false,
        isRemoved: node.isremoved
      };
      console.log('New Node:');
      console.log(newNode);
      //Determine this is the first node
      if(!root){
        console.log('putting root');
        yield pushNode(newNode);
        return;
      }

      //Get new position
      console.log('Looking for new position: ', newNode.relatedabove);
      getNewPosition(newNode.relatedabove).then(wrapper(function*(pos){
        console.log(pos);
        if(!pos){
          return console.log("Limit exceeded!");
        }

        let directAboveNode = yield getNodeFromId(pos.directAbove);

        newNode.path = directAboveNode.path + (pos.position === PositionType.LEFT ? '0' : '1');
        newNode.directAbove = pos.directAbove;

        yield pushNode(newNode);

        /* Update parent nodes */

        var params;
        //Update direct above node
        if(pos.position === PositionType.LEFT){
          // directAboveNode.relatedLeft = newNode.uniqueid;
          params = {
            TableName: "table6_relatedrecords",
            Key: {
              uniqueid: directAboveNode.uniqueid,
              paymentdatehourminutesecond: directAboveNode.paymentdatehourminutesecond
            },
            UpdateExpression: 'SET relatedLeft = :value',
            ExpressionAttributeValues : {
              ':value' : newNode.uniqueid
            }
          };
          yield docClient.update(params).promise();
        } else if(pos.position === PositionType.RIGHT){
          // directAboveNode.relatedRight = newNode.uniqueid;
          params = {
            TableName: "table6_relatedrecords",
            Key: {
              uniqueid: newNode.directAbove,
              paymentdatehourminutesecond: directAboveNode.paymentdatehourminutesecond
            },
            UpdateExpression: 'SET relatedRight = :value',
            ExpressionAttributeValues : {
              ':value' : newNode.uniqueid
            }
          };
          yield docClient.update(params).promise();
        }

        //Save directNode to database

        //Update related above nodes(relatedLeftCount, relatedRightCount)
        let current = newNode;
        const maxLimit = limits[limits.length - 1];
        while(true){
          if(current.directAbove === undefined)
            break;
          let directAbove = yield getNodeFromId(current.directAbove);

          //Determine current node is in left or right of directAbove
          if(directAbove.relatedLeft === current.uniqueid){
            params = {
              TableName: "table6_relatedrecords",
              Key: {
                uniqueid: directAbove.uniqueid,
                paymentdatehourminutesecond: directAbove.paymentdatehourminutesecond
              },
              UpdateExpression: 'SET relatedLeftCount = relatedLeftCount + :value',
              ConditionExpression: 'fullHouse=:f',
              ExpressionAttributeValues : {
                ':value': 1,
                ':f': false
              }
            };
            yield docClient.update(params).promise()
              .then(wrapper(function*(data){
                const params = {
                  TableName: "table6_relatedrecords",
                  Key: {
                    uniqueid: directAbove.uniqueid,
                    paymentdatehourminutesecond: directAbove.paymentdatehourminutesecond
                  },
                  UpdateExpression: 'SET fullHouse = :value',
                  ConditionExpression: 'relatedLeftCount=:limit AND relatedRightCount=:limit',
                  ExpressionAttributeValues : {
                    ':value': true,
                    ':limit': maxLimit
                  }
                };
                yield docClient.update(params).promise();
              }))
              .catch(err => {
              })
            ;
          }

          if(directAbove.relatedRight === current.uniqueid){
            params = {
              TableName: "table6_relatedrecords",
              Key: {
                uniqueid: directAbove.uniqueid,
                paymentdatehourminutesecond: directAbove.paymentdatehourminutesecond
              },
              UpdateExpression: 'SET relatedRightCount = relatedRightCount + :value',
              ConditionExpression: 'fullHouse=:f',
              ExpressionAttributeValues : {
                ':value': 1,
                ':f': false
              }
            };
            yield docClient.update(params).promise()
              .then(wrapper(function*(data){
                const params = {
                  TableName: "table6_relatedrecords",
                  Key: {
                    uniqueid: directAbove.uniqueid,
                    paymentdatehourminutesecond: directAbove.paymentdatehourminutesecond
                  },
                  UpdateExpression: 'SET fullHouse = :value',
                  ConditionExpression: 'relatedLeftCount=:limit AND relatedRightCount=:limit',
                  ExpressionAttributeValues : {
                    ':value': true,
                    ':limit': maxLimit
                  }
                };
                yield docClient.update(params).promise();
              }))
              .catch(err => {});
          }


          //Store directAbove node

          current = directAbove;
        }
      }));
    })).catch(err => {
      console.log(err);
    });
  });
}

function markAsRemoved(removeNode){
  return new Promise((resolve, reject) => {
    getNodeFromId(removeNode.uniqueid)
      .then(node => {
        const params = {
          TableName: "table6_relatedrecords",
          Key: {
            uniqueid: node.uniqueid,
            paymentdatehourminutesecond: node.paymentdatehourminutesecond
          },
          UpdateExpression: 'SET isRemoved = :value',
          ExpressionAttributeValues : {
            ':value' : removeNode.isremoved
          }
        };
        resolve(docClient.update(params).promise().then());
      })
      .catch(err => reject(err));
  })
}
//Read table8 references
function readLimits(){
  const params = {
    TableName: 'table8_reference'
  };
  return docClient.scan(params).promise().then(res => res.Items);
}

var events = {
  "Records": [
    {
      "eventID": "1",
      "eventVersion": "1.0",
      "dynamodb": {
        "Keys": {
          "uniqueid": {
            "S": "2"
          }
        },
        "NewImage": {
          "paymentdatehourminutesecond": {
            "S": "2017-06-13"
          },
          "relatedabove": {
            "S": "0"
          },
          "paymentstatus": {
            "BOOL": true
          },
          "uniqueid": {
            "S": "25"
          }
        },
        "StreamViewType": "NEW_AND_OLD_IMAGES",
        "SequenceNumber": "111",
        "SizeBytes": 26
      },
      "awsRegion": "us-west-2",
      "eventName": "INSERT",
      "eventSourceARN": "arn:aws:dynamodb:us-west-2:account-id:table/ExampleTableWithStream/stream/2015-06-27T00:48:05.899",
      "eventSource": "aws:dynamodb"
    }
  ]};

var handler = function(event, context){
  var buffer = {};
  var inflightRequests = 0;
  event.Records.forEach((record) => {
    buffer[JSON.stringify(record.dynamodb.Keys)] = record.dynamodb;
  });

  var handleResponse = function(err, data){
    if(err){
      //log errors
      console.error(err, err.stack);
    } else {
      //check if all requests are processed, if so, end function
      inflightRequests--;
      if (inflightRequests === 0){
        context.succeed(`Successfully processed ${event.Records.length} records.`);
      }
    }
  };

  readLimits().then(res => {
    console.log(res);
    res.sort((a, b) => {
      if(a.point > b.point)
        return 1;
      if(a.point < b.point)
        return -1;
      return 0;
    });
    limits = [];
    res.forEach(function(a){
      limits.push(a.relatedbelow01number);
    });
    console.log("Limits: ", limits);
  }).then(() => {
    for (var key in buffer){
      if (!buffer.hasOwnProperty(key)) continue;
      //Get new image of the DynamoDB stream record
      var oldItemImage = attr.unwrap(buffer[key].OldImage || '');
      var newItemImage = attr.unwrap(buffer[key].NewImage || '');
      console.log('Old Image: ', oldItemImage);
      console.log('New Image: ', newItemImage);
      if(validate(newItemImage)){

        var isAdd = (oldItemImage.paymentstatus === false && newItemImage.paymentstatus === true);
        isAdd = isAdd || (oldItemImage.paymentstatus !== true && newItemImage.paymentstatus === true);
        if(isAdd){
          getNodeFromId(newItemImage.uniqueid)
            .then(node => {
              if(node){
                console.log("Already exists");
              } else {
                addNode(newItemImage)
                  .then(() => {
                    var isRemoved = newItemImage.isremoved;
                    if(isRemoved || (oldItemImage.isremoved != newItemImage.isremoved)){
                      markAsRemoved(newItemImage)
                        .then(() => handleResponse(undefined))
                        .catch(err => handleResponse(err))
                    }
                  })
                  .catch(err => handleResponse(err));
              }
            })
            .catch(err => {
              console.error(err, err.stack);
            })
        } else {
          var isRemoved = newItemImage.isremoved;
          if(isRemoved || (oldItemImage.isremoved != newItemImage.isremoved)){
            markAsRemoved(newItemImage)
              .then(() => handleResponse(undefined))
              .catch(err => handleResponse(err))
          }
          console.log('PaymentStatus is not verified');
        }
      } else {
        console.log('No need to process');
      }
      inflightRequests++;
    }
  }).catch(err => {
    console.error(err, err.stack);
  });
};

module.exports.handler = handler;
var validate = function(image){
  if(typeof image !== 'undefined' && image)
    return true;
  return false;
};
//
// handler(events, {
//   succeed: function(res){
//     console.log("Succeeded");
//     console.log(res);
//   }
// });