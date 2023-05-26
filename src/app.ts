import * as dotenv from "dotenv";
dotenv.config();

import { MongoClient } from "mongodb";
import { faker } from "@faker-js/faker";
import { Customer } from "./interface/customer.interface";

const { DB_URI, CUSTOMERS_COLLECTION = "customers" } = process.env;

const client = new MongoClient(DB_URI as string);
const customersRepository = client
  .db()
  .collection<Customer>(CUSTOMERS_COLLECTION as string);

const createRandomCustomer = (): Customer => ({
  firstName: faker.person.firstName(),
  lastName: faker.person.lastName(),
  email: faker.internet.email(),
  createdAt: faker.date.past(),
  address: {
    line1: faker.location.streetAddress(),
    line2: faker.location.secondaryAddress(),
    postcode: faker.location.zipCode(),
    city: faker.location.city(),
    state: faker.location.state(),
    country: faker.location.country(),
  },
});

setInterval(async () => {
  const customers: Customer[] = faker.helpers.multiple(createRandomCustomer, {
    count: faker.number.int({ min: 1, max: 10 }),
  });
  console.log(`----- ${customers.length} NEW CUSTOMERS CREATED -----`);

  try {
    await customersRepository.insertMany(customers);

    console.log(
      `NEW CUSTOMERS SUCCESSFULLY INSERTED WITH IDS:\n${customers
        .map((customer) => `   ${customer._id}`)
        .join("\n")}`
    );
  } catch (err) {
    console.log(`CUSTOMERS INSERTING ERROR ${err}`);
  }
}, 200);
