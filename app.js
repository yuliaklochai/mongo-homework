'use strict';

const {mapUser, getRandomFirstName, mapArticle} = require('./util');
const studentsList = require('./students.json');

// db connection and settings
const connection = require('./config/connection');
let userCollection;
let articlesCollection;
let studentsCollection;
run();

async function run() {
  await connection.connect();
  await connection.get().dropCollection('users');
  await connection.get().createCollection('users');
  userCollection = connection.get().collection('users');

  await connection.get().dropCollection('articles');
  await connection.get().createCollection('articles');
  articlesCollection = connection.get().collection('articles');

  await connection.get().dropCollection('students');
  await connection.get().createCollection('students');
  studentsCollection = connection.get().collection('students');

//Users

  await example1();
  await example2();
  await example3();
  await example4();

//Articles

  // await example5();
  // await example6();
  // await example7();
  // await example8();
  // await example9();

//Students

  // await example10();
  // await example11();
  // await example12();
  // await example13();
  // await example14();
  // await example15();
  // await example16();
  // await example17();
  await connection.close();
}

// #### Users

// - Create 2 users per department (a, b, c)
async function example1() {
  try {
    const users = [];
    const departments = ['a', 'a', 'b', 'b', 'c', 'c'];
    departments.forEach(dep => {
      let user = mapUser();
      user.department = dep;
      users.push(user);
    });
    await userCollection.insertMany(users);
  } catch (err) {
    console.error(err);
  }
}

// - Delete 1 user from department (a)

async function example2() {
  try {
    await userCollection.deleteOne({department: 'a'});
  } catch (err) {
    console.error(err);
  }
}

// - Update firstName for users from department (b)

async function example3() {
  try {
    await userCollection.updateMany(
      {department: 'b'},
      {
        $set: {firstName: getRandomFirstName()}
      }
    );
  } catch (err) {
    console.error(err);
  }
}

// - Find all users from department (c)
async function example4() {
  try {
    let usersFromC = await userCollection.find({department: 'c'}).toArray();
  } catch (err) {
    console.error(err);
  }
}

// #### Articles

// - Create 5 articles per each type (a, b, c)

async function example5() {
  try {
    const articles = [];
    const typeList = ['a', 'b', 'c'];
    typeList.forEach(type => {
      for (let i = 0; i < 5; i++) {
        let article = mapArticle();
        article.type = type;
        articles.push(article);
      }
    });
    await articlesCollection.insertMany(articles);
  } catch (err) {
    console.error(err);
  }
}

// - Find articles with type a, and update tag list with next value [‘tag1-a’, ‘tag2-a’, ‘tag3’]

async function example6() {
  try {
    await articlesCollection.updateMany(
      {type: 'a'},
      {
        $set: {tags: ['tag1-a', 'tag2-a', 'tag3']}
      }
    );
    let articles = await articlesCollection.find({type: 'a'}).toArray();
  } catch (err) {
    console.error(err);
  }
}

// - Add tags [‘tag2’, ‘tag3’, ‘super’] to other articles except articles from type a

async function example7() {
  try {
    await articlesCollection.updateMany(
      {type: {$ne: 'a'}},
      {
        $set: {tags: ['tag2', 'tag3', 'super']}
      }
    );
  } catch (err) {
    console.error(err);
  }
}

// - Find all articles that contains tags [tag2, tag1-a]

async function example8() {
  try {
    let articles = await articlesCollection.find({tags: {$in: ['tag2', 'tag1-a']}}).toArray();
  } catch (err) {
    console.error(err);
  }
}

// - Pull [tag2, tag1-a] from all articles

async function example9() {
  try {
    await articlesCollection.updateMany({}, {$pull: {tags: {$in: ['tag2', 'tag1-a']}}});
  } catch (err) {
    console.error(err);
  }
}

// #### Students

// - Import all data from students.json into student collection

async function example10() {
  try {
    await studentsCollection.insertMany(studentsList);
  } catch (err) {
    console.error(err);
  }
}

// - Find all students who have the worst score for homework, sort by descent

async function example11() {
  try {
    await studentsCollection.aggregate([
      {$unwind: {path: '$scores'}},
      {$match: {'scores.type': 'homework'}},
      {$sort: {'scores.score': 1}},
      {$limit: 5},
      {$sort: {'scores.score': -1}}
    ]);
  } catch (err) {
    console.error(err);
  }
}

// - Find all students who have the best score for quiz and the worst for homework, sort by ascending

async function example12() {
  try {
    await studentsCollection.aggregate([
      {
        $project: {
          name: 1,
          maxScore: {$max: '$scores.score'},
          minScore: {$min: '$scores.score'},
          scores: 1
        }
      },
      {$unwind: {path: '$scores'}},
      {
        $match: {
          $or: [
            {
              $and: [{$expr: {$eq: ['$scores.score', '$maxScore']}}, {'scores.type': 'quiz'}]
            },
            {
              $and: [{$expr: {$eq: ['$scores.score', '$minScore']}}, {'scores.type': 'homework'}]
            }
          ]
        }
      },
      {
        $group: {
          _id: {_id: '$_id', name: '$name'},
          scores: {
            $push: {
              score: '$scores.score',
              type: '$scores.type'
            }
          }
        }
      },
      {
        $match: {
          scores: {$size: 2}
        }
      },
      {
        $project: {
          _id: '$_id._id',
          name: '$_id.name',
          scores: 1
        }
      },
      {
        $sort: {
          'scores.score': 1
        }
      }
    ]);
  } catch (err) {
    console.error(err);
  }
}

// - Find all students who have best scope for quiz and exam

async function example13() {
  try {
    await studentsCollection.aggregate([
      {
        $project: {
          name: 1,
          minScore: {$min: '$scores.score'},
          scores: 1
        }
      },
      {$unwind: {path: '$scores'}},
      {
        $match: {
          $and: [{$expr: {$gt: ['$scores.score', '$minScore']}}, {'scores.type': {$ne: 'homework'}}]
        }
      },
      {
        $group: {
          _id: {_id: '$_id', name: '$name'},
          scores: {
            $push: {
              score: '$scores.score',
              type: '$scores.type'
            }
          }
        }
      },
      {
        $match: {
          scores: {$size: 2}
        }
      },
      {
        $project: {
          _id: '$_id._id',
          name: '$_id.name',
          scores: 1
        }
      }
    ]);
  } catch (err) {
    console.error(err);
  }
}

// - Calculate the average score for homework for all students

async function example14() {
  try {
    await studentsCollection.aggregate([
      {$unwind: {path: '$scores'}},
      {$match: {'scores.type': 'homework'}},
      {
        $group: {
          _id: 'homework',
          avarageScore: {
            $avg: '$scores.score'
          }
        }
      }
    ]);
  } catch (err) {
    console.error(err);
  }
}

// - Delete all students that have homework score <= 60

async function example15() {
  try {
    const studentsList = await studentsCollection
      .aggregate([
        {$unwind: {path: '$scores'}},
        {
          $match: {
            $and: [{$expr: {$lte: ['$scores.score', 60]}}, {'scores.type': 'homework'}]
          }
        },
        {$project: {_id: '$_id'}}
      ])
      .toArray();
    const studentsID = studentsList.map(stud => stud._id);
    await studentsCollection.deleteMany({_id: {$in: studentsID}});
  } catch (err) {
    console.error(err);
  }
}

// - Mark students that have quiz score => 80

async function example16() {
  try {
    const studentsList = await studentsCollection
      .aggregate([
        {$unwind: {path: '$scores'}},
        {
          $match: {
            $and: [{$expr: {$gte: ['$scores.score', 80]}}, {'scores.type': 'quiz'}]
          }
        },
        {$project: {_id: '$_id'}}
      ])
      .toArray();
    const studentsID = studentsList.map(stud => stud._id);
    await studentsCollection.updateMany({_id: {$in: studentsID}}, {$set: {quizScoreGt80: true}});
  } catch (err) {
    console.error(err);
  }
}

// - Write a query that group students by 3 categories (calculate the average grade for three subjects)
// - a => (between 0 and 40)
// - b => (between 40 and 60)
// - c => (between 60 and 100)

async function example17() {
  try {
    await studentsCollection.aggregate([
      {$project: {name: 1, avarageGrade: {$avg: '$scores.score'}}},
      {
        $bucket: {
          groupBy: '$avarageGrade',
          boundaries: [0, 40, 60, 100],
          default: 'Other',
          output: {
            students: {
              $push: {
                name: '$name',
                avarageGrade: '$avarageGrade'
              }
            }
          }
        }
      }
    ]);
  } catch (err) {
    console.error(err);
  }
}
