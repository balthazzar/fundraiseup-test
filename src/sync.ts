import * as dotenv from "dotenv";
dotenv.config();

import {
  AnyBulkWriteOperation,
  ChangeStreamOptions,
  ResumeToken,
} from "mongodb";
import { Customer } from "./interface/customer.interface";
import { anonymiseCustomer } from "./util/anonymise.util";
import {
  commitAnonymisedCustomers,
  customersAnonymisedRepository,
  customersRepository,
  getResumeToken,
} from "./util/db.util";

const forceFullReindex = process.argv.indexOf("--full-reindex") !== -1;
const { BUNDLE_MAX_SIZE = 1000 } = process.env;

const fullReindex = async () => {
  console.log(`----- START DATA TRANSFER -----`);

  const customersCursor = customersRepository.find({});

  let customersForTransfer: AnyBulkWriteOperation<Customer>[] = [];

  for await (const customer of customersCursor) {
    customersForTransfer.push({
      updateOne: {
        filter: { _id: customer._id },
        update: { $set: anonymiseCustomer(customer) },
        upsert: true,
      },
    });

    if (customersForTransfer.length >= +BUNDLE_MAX_SIZE) {
      await customersAnonymisedRepository.bulkWrite(customersForTransfer);
      console.log(
        `${customersForTransfer.length} DOCUMENTS SUCCESSFULLY TRANSFERED`
      );

      customersForTransfer = [];
    }
  }

  await customersAnonymisedRepository.bulkWrite(customersForTransfer);
  console.log(
    `${customersForTransfer.length} DOCUMENTS SUCCESSFULLY TRANSFERED`
  );
};

const sync = () => {
  const restoredResumeToken: ResumeToken = getResumeToken();
  let opsBundle: AnyBulkWriteOperation<Customer>[] = [];

  const watchOptions: ChangeStreamOptions = {};

  if (restoredResumeToken) {
    console.log("----- RESUME SYNC FROM STORED POINT -----");

    watchOptions.resumeAfter = restoredResumeToken;
  } else {
    console.log("----- RESUME POINT NOT FOUND. START NEW SYNC -----");
  }

  const stream = customersRepository.watch([], watchOptions);

  stream.on("change", async (changeEvent) => {
    console.log(
      `NEW ${changeEvent.operationType} OPERATION\n ${
        (changeEvent as any).documentKey?._id
      }`
    );

    switch (changeEvent.operationType) {
      case "insert":
        opsBundle.push({
          insertOne: {
            document: anonymiseCustomer(changeEvent.fullDocument) as Customer,
          },
        });
        break;
      case "update":
        opsBundle.push({
          updateOne: {
            filter: { _id: changeEvent.documentKey._id },
            update: {
              $set: anonymiseCustomer(
                changeEvent.updateDescription.updatedFields || {}
              ),
            },
          },
        });
        break;
    }

    if (opsBundle.length >= +BUNDLE_MAX_SIZE) {
      timeout.refresh();

      await commitAnonymisedCustomers(opsBundle, stream.resumeToken);
      opsBundle = [];
    }
  });

  const timeout = setTimeout(async () => {
    if (opsBundle.length) {
      await commitAnonymisedCustomers(opsBundle, stream.resumeToken);
      opsBundle = [];
    }

    timeout.refresh();
  }, 1000);
};

if (forceFullReindex) {
  (async () => {
    try {
      await fullReindex();

      console.log("----- ALL DATA SUCCESSFULLY TRANSFERED -----");
      process.exit(0);
    } catch (err) {
      console.log(`ERROR OCCURED DURING FULL REINDEX: ${err}`);
      process.exit(1);
    }
  })();
} else {
  try {
    sync();
  } catch (err) {
    console.log(`ERROR OCCURED DURING DB WATCH: ${err}`);
  }
}
