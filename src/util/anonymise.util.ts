import crypto from 'crypto';

import { Address } from '../interface/address.interface';
import { Customer } from '../interface/customer.interface';

const hash = (input: string): string => {
  const hash = crypto.createHash('shake256', { outputLength: 6 }).update(input).digest('base64url');

  return hash.replace('-', 'A').replace('_', 'B');
}

export const anonymiseCustomer = (customer: Customer | Partial<Customer>): Customer | Partial<Customer> => {
  if (!customer) {
    return {};
  }

  const anonedCustomer: Customer | Partial<Customer> = {
    ...customer
  };

  for (let key of Object.keys(customer)) {
    switch (key) {
      case 'firstName':
      case 'lastName':
        anonedCustomer[key] = hash(customer[key] as string);
        break;
      case 'address': {
        anonedCustomer.address ??= {};

        for (let addressKey of Object.keys(customer.address as Address | Partial<Address>)) {
          switch (addressKey) {
            case 'line1':
            case 'line2':
            case 'postcode': {
              anonedCustomer.address[addressKey] = hash((customer.address && customer.address[addressKey]) as string);
              break;
            }
          }
        }

        break;
      }
      case 'email': {
        const splitedEmail = customer.email?.split('@') ?? [];

        anonedCustomer[key] = `${hash(splitedEmail[0])}@${splitedEmail[1]}`;

        break;
      }
    }
  }
  
  return anonedCustomer;
};
