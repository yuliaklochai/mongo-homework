'use strict'

const {mapUser, getRandomFirstName} = require('./util')

// db connection and settings
const connection = require('./config/connection')
let userCollection
run()

async function run() {
  await connection.connect()
  await connection.get().dropCollection('users')
  await connection.get().createCollection('users')
  userCollection = connection.get().collection('users')

  await example1()
  await example2()
  await example3()
  await example4()
  console.log('111');
  await connection.close()
}

// #### Users

// - Create 2 users per department (a, b, c)
async function example1() {

  try {

  } catch (err) {
    console.error(err)
  }
}

// - Delete 1 user from department (a)

async function example2() {
  try {

  } catch (err) {
    console.error(err)
  }
}

// - Update firstName for users from department (b)

async function example3() {
  try {

  } catch (err) {
    console.error(err)
  }
}

// - Find all users from department (c)
async function example4() {
  try {

  } catch (err) {
    console.error(err)
  }
}
