import * as dotenv from "dotenv";
dotenv.config();

import { writeFile } from "fs/promises";
import { AnyBulkWriteOperation, ResumeToken, MongoClient } from "mongodb";
import { Customer } from "../interface/customer.interface";
import { existsSync, readFileSync } from "fs";

const {
  DB_URI,
  CUSTOMERS_COLLECTION = "customers",
  CUSTOMERS_ANONYMISED_COLLECTION = "customers_anonymised",
  DB_METADATA_FILENAME = "mongo-meta.json",
} = process.env;

const client = new MongoClient(DB_URI as string);
const database = client.db();

export const customersRepository =
  database.collection<Customer>(CUSTOMERS_COLLECTION);
export const customersAnonymisedRepository = database.collection<Customer>(
  CUSTOMERS_ANONYMISED_COLLECTION
);

export const getResumeToken = () => {
  existsSync(DB_METADATA_FILENAME) &&
    JSON.parse(readFileSync(DB_METADATA_FILENAME, { encoding: "utf-8" }));

  let restoredResumeToken: ResumeToken;

  try {
    restoredResumeToken =
      existsSync(DB_METADATA_FILENAME) &&
      JSON.parse(readFileSync(DB_METADATA_FILENAME, { encoding: "utf-8" }));
  } catch (err) {
    console.log(`ERROR OCCURED DURING RESTORE RESUME TOKEN: ${err}`);
  }

  return restoredResumeToken;
};

export const commitAnonymisedCustomers = async (
  opsBundle: AnyBulkWriteOperation<Customer>[],
  resumeToken: ResumeToken
) => {
  try {
    await customersAnonymisedRepository.bulkWrite(opsBundle);
    await writeFile(DB_METADATA_FILENAME, JSON.stringify(resumeToken));
  } catch (err) {
    console.log(`ERROR OCCURED DURING ANONYMISED DATA UPDATE: ${err}`);
  }
};
