import * as dotenv from 'dotenv';
dotenv.config()

import { existsSync, readFileSync } from 'fs';
import { AnyBulkWriteOperation, ChangeStreamOptions, MongoClient, ResumeToken } from 'mongodb';
import { Customer } from './interface/customer.interface';
import { anonymiseCustomer } from './util/anonymise.util';
import { commit, fullReindex } from './util/db.util';

const forceFullReindex = process.argv.indexOf('--full-reindex') !== -1;
const {
  DB_URI,
  CUSTOMERS_COLLECTION = 'customers',
  CUSTOMERS_ANONYMISED_COLLECTION = 'customers_anonymised',
  DB_METADATA_FILENAME = 'mongo-meta.json',
  BUNDLE_MAX_SIZE = 1000
} = process.env;

const client = new MongoClient(DB_URI as string);
const database = client.db();
const customersRepository = database.collection<Customer>(CUSTOMERS_COLLECTION);
const customersAnonymisedRepository = database.collection<Customer>(CUSTOMERS_ANONYMISED_COLLECTION);

if (forceFullReindex) {
  (async () => {
    try {
      await fullReindex(customersRepository, customersAnonymisedRepository);
      
      console.log('----- ALL DATA SUCCESSFULLY TRANSFERED -----');
      process.exit(0);
    } catch (err) {
      console.log(`ERROR OCCURED DURING FULL REINDEX: ${err}`);
      process.exit(1);
    };
  })();
} else {
  let opsBundle: AnyBulkWriteOperation<Customer>[] = [];
  let restoredResumeToken: ResumeToken;
  
  try {
    restoredResumeToken = existsSync(DB_METADATA_FILENAME)
      && JSON.parse(readFileSync(DB_METADATA_FILENAME, { encoding: 'utf-8' }));
  } catch (err) {
    console.log(`ERROR OCCURED DURING RESTORE RESUME TOKEN: ${err}`);
  }
  
  try {
    const watchOptions: ChangeStreamOptions = {};
    
    if (restoredResumeToken) {
      console.log('----- RESUME SYNC FROM STORED POINT -----')
  
      watchOptions.resumeAfter = restoredResumeToken;
    } else {
      console.log('----- RESUME POINT NOT FOUND. START NEW SYNC -----')
    }
    
    const stream = customersRepository.watch([], watchOptions);
    
    stream.on('change', (changeEvent) => {
      console.log(`NEW ${changeEvent.operationType} OPERATION\n ${(changeEvent as any).documentKey?._id}`);
  
      switch (changeEvent.operationType) {
        case 'insert':
          opsBundle.push({ insertOne: { document: anonymiseCustomer(changeEvent.fullDocument) as Customer } });
          break;
        case 'update':
          opsBundle.push({
            updateOne: {
              filter: { _id: changeEvent.documentKey._id },
              update: { $set: anonymiseCustomer(changeEvent.updateDescription.updatedFields || {}) }
            }
          });
          break;
      }
  
      if (opsBundle.length >= +BUNDLE_MAX_SIZE) {
        timeout.refresh();
  
        commit(customersAnonymisedRepository, opsBundle, stream.resumeToken);
        opsBundle = [];
      }
    });
  
    const timeout = setTimeout(() => {
      if (opsBundle.length) {
        commit(customersAnonymisedRepository, opsBundle, stream.resumeToken);
        opsBundle = [];
      }
  
      timeout.refresh();
    }, 1000);
  } catch (err) {
    console.log(`ERROR OCCURED DURING DB WATCH: ${err}`);
  }
}