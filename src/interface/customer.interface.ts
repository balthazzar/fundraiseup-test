import { ObjectId } from "mongodb";
import { Address } from "./address.interface";

export interface Customer {
  _id?: ObjectId;
  firstName: string;
  lastName: string;
  email: string;
  address: Address | Partial<Address>;
  createdAt: Date;
}
