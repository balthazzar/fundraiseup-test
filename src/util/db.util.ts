import * as dotenv from 'dotenv';
dotenv.config()

import { writeFile } from 'fs/promises';
import { Collection, AnyBulkWriteOperation, ResumeToken } from 'mongodb';
import { Customer } from '../interface/customer.interface';
import { anonymiseCustomer } from './anonymise.util';

const { DB_METADATA_FILENAME = 'mongo-meta.json', BUNDLE_MAX_SIZE = 1000 } = process.env;

export const commit = async (commitRepository: Collection<Customer>, opsBundle: AnyBulkWriteOperation<Customer>[], resumeToken: ResumeToken) => {
  try {
    await commitRepository.bulkWrite(opsBundle);
    await writeFile(DB_METADATA_FILENAME, JSON.stringify(resumeToken));
  } catch (err) {
    console.log(`ERROR OCCURED DURING ANONYMISED DATA UPDATE: ${err}`);
  }
};

export const fullReindex = async (customersRepository: Collection<Customer>, customersAnonymisedRepository: Collection<Customer>) => {
  console.log(`----- START DATA TRANSFER -----`);
  
  const customersCursor = customersRepository.find({});

  let customersForTransfer: AnyBulkWriteOperation<Customer>[] = [];

  for await (const customer of customersCursor) {
    customersForTransfer.push({
      updateOne: {
        filter: { _id: customer._id },
        update: { $set: anonymiseCustomer(customer) },
        upsert: true
      }
    });

    if (customersForTransfer.length >= +BUNDLE_MAX_SIZE) {
      await customersAnonymisedRepository.bulkWrite(customersForTransfer);
      console.log(`${customersForTransfer.length} DOCUMENTS SUCCESSFULLY TRANSFERED`);
      
      customersForTransfer = [];
    }
  }

  await customersAnonymisedRepository.bulkWrite(customersForTransfer);
  console.log(`${customersForTransfer.length} DOCUMENTS SUCCESSFULLY TRANSFERED`);
};
