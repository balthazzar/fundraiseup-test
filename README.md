# fundraiseup-test

Fundraiseup backend test task

## Build application

`npm run build`

## Start customer spawner (app.ts)

`npm run start:app`

## Start watcher in sync mode spawner (sync.ts)

`npm run start:sync`

## Start watcher in reindex mode (sync.ts --full-reindex)

`npm run start:reindex`

## .env file

There are several optional parameter you are able to pass (default values in `.env.example`):

- `CUSTOMERS_COLLECTION` - customers collection name
- `CUSTOMERS_ANONYMISED_COLLECTION` - customers_anonymised collection name
- `DB_METADATA_FILENAME` - filename for storing resume token
- `BUNDLE_MAX_SIZE` - maximum size of database operations bundle
